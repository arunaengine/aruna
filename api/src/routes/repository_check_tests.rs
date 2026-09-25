//! Tests the repository kinds listing and the dataset check against a repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::repository_links::tests::{Linked, setup, setup_crate};
use aruna_core::metadata::{INVENIO_PROFILE_IRI, ZENODO_PROFILE_IRI};

async fn check(linked: &Linked, request: CheckRequest) -> ServerResult<CheckResponse> {
    Box::pin(check_repository(
        State(linked.test.state.clone()),
        Extension(Some(linked.test.auth.clone())),
        Path(linked.document_id.clone()),
        Json(request),
    ))
    .await
    .map(|Json(checked)| checked)
}

fn request(linked: &Linked, metadata: Option<serde_json::Value>) -> CheckRequest {
    CheckRequest {
        group_id: linked.test.group_id.to_string(),
        connector_id: linked.connector_id.to_string(),
        metadata,
    }
}

#[tokio::test]
async fn kinds_list_publishers() {
    let linked = setup().await;
    let state = || State(linked.test.state.clone());
    assert!(matches!(
        list_kinds(state(), Extension(None)).await,
        Err(ServerError::Unauthorized)
    ));
    let Json(kinds) = list_kinds(state(), Extension(Some(linked.test.auth.clone())))
        .await
        .unwrap();
    let kinds = serde_json::to_value(kinds).unwrap();
    // oai_pmh only harvests, so only invenio is listed.
    assert_eq!(kinds.as_array().unwrap().len(), 1);
    let invenio = &kinds[0];
    assert_eq!(invenio["kind"], "invenio");
    assert_eq!(invenio["capabilities"]["identifier_kind"], "doi");
    assert_eq!(invenio["capabilities"]["release_date"], false);
    let profiles = invenio["profiles"].as_array().unwrap();
    for iri in [ZENODO_PROFILE_IRI, INVENIO_PROFILE_IRI] {
        let profile = profiles
            .iter()
            .find(|profile| profile["iri"] == iri)
            .unwrap();
        assert!(
            profile["name"]
                .as_str()
                .is_some_and(|name| !name.is_empty())
        );
        assert!(
            profile["shapes"][0]
                .as_str()
                .unwrap()
                .contains("sh:NodeShape")
        );
    }
    assert_eq!(invenio["targets"][0]["name"], "record");
    assert_eq!(invenio["targets"][1]["content"]["max_files"], 100);
}

#[tokio::test]
async fn check_reports_findings() {
    let root = serde_json::json!({"@id": "./", "@type": "Dataset", "name": "Bare",
        "description": "No creators", "datePublished": "2026-01-01", "publisher": "Aruna test"});
    let linked = setup_crate(root, "https://rdm.example.org/api/").await;
    // Overrides that name creators do not make the crate ready.
    let overrides = serde_json::json!({"creators": [
        {"person_or_org": {"type": "personal", "family_name": "Doe"}}]});
    let checked = check(&linked, request(&linked, Some(overrides)))
        .await
        .unwrap();
    assert_eq!(checked.kind, "invenio");
    assert_eq!(checked.profile.iri, INVENIO_PROFILE_IRI);
    assert!(!checked.ready);
    assert!(checked.findings.iter().any(|finding| {
        finding.path.as_deref()
            == Some("(<http://schema.org/author> | <http://schema.org/creator>)")
            && finding.severity == "violation"
    }));
    assert!(
        checked
            .mapping
            .iter()
            .any(|mapped| mapped.entity_id == "./" && mapped.target == "record")
    );
    let not_object = check(&linked, request(&linked, Some(serde_json::json!([])))).await;
    assert!(matches!(not_object, Err(ServerError::BadRequestReason(_))));
    let mut unknown = request(&linked, None);
    unknown.connector_id = ulid::Ulid::generate().to_string();
    assert!(matches!(
        check(&linked, unknown).await,
        Err(ServerError::NotFound)
    ));
}

#[tokio::test]
async fn check_passes_complete() {
    let linked = setup().await;
    let checked = check(&linked, request(&linked, None)).await.unwrap();
    assert!(checked.ready, "findings: {:?}", checked.findings);
    assert!(
        checked
            .findings
            .iter()
            .all(|finding| finding.severity != "violation")
    );
}
