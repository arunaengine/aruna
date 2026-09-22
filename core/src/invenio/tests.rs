//! Checks repository identity and preservation of version-specific file references.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

#[test]
fn rejects_unsafe_ids() {
    for id in ["", "../42", "42?token=secret", "a/b", "%2e", "a\\b"] {
        assert!(validate_id(id).is_err());
    }
    assert!(validate_id("abc12-34567").is_ok());
}

#[test]
fn separates_version_files() {
    assert_ne!(
        file_path("1", "data.csv").unwrap(),
        file_path("2", "data.csv").unwrap()
    );
    assert!(!file_path("1", "../../escape").unwrap().contains(".."));
    assert_ne!(
        file_path("1", "a/b").unwrap(),
        file_path("1", "a%2Fb").unwrap()
    );
}

#[test]
fn preserves_version_identifiers() {
    let records: Vec<_> = ["1", "2"].into_iter().map(|id| (
        json!({"id": id, "metadata": {"title": format!("Version {id}"), "version": id, "publication_date": "2024-01-01"},
            "pids": {"doi": {"identifier": format!("10.1234/{id}")}},
            "parent": {"pids": {"doi": {"identifier": "10.1234/all"}}}}),
        json!({"entries": [{"key": "data.csv", "size": 4, "file_id": format!("file-{id}")}]}),
    )).collect();
    let document = import_crate("https://zenodo.org/api/", "2", &records).unwrap();
    let graph = document["@graph"].as_array().unwrap();
    let root = graph.iter().find(|entry| entry["@id"] == "./").unwrap();
    assert_eq!(root["name"], "Version 2");
    assert_eq!(root["hasPart"].as_array().unwrap().len(), 2);
    for id in ["1", "2"] {
        let version = graph
            .iter()
            .find(|entry| entry["@id"] == format!("versions/{id}/"))
            .unwrap();
        assert_eq!(version["identifier"][0]["value"], format!("10.1234/{id}"));
        assert_eq!(version["identifier"][1]["value"], "10.1234/all");
        assert_eq!(version["hasPart"].as_array().unwrap().len(), 2);
    }
    craqle::validate_rocrate_jsonld(&document.to_string()).unwrap();
}

#[test]
fn requires_selected_version() {
    assert!(import_crate("https://example.org/api/", "9", &[]).is_err());
}

#[test]
fn maps_creator_identifiers() {
    let entity = record_entity(
        &json!({
            "id": "1", "created": "2024-01-01T00:00:00Z", "updated": "2025-01-01T00:00:00Z",
            "metadata": {"title": "Record", "subjects": [{"subject": "Genomics"}],
                "creators": [{"person_or_org": {"type": "personal", "name": "Researcher",
                    "identifiers": [{"scheme": "orcid", "identifier": "0000-0002-1825-0097"}]},
                    "affiliations": [{"id": "01ggx4157", "name": "CERN"}]}]}
        }),
        "./",
    )
    .unwrap();
    assert_eq!(entity["dateCreated"], "2024-01-01T00:00:00Z");
    assert_eq!(entity["dateModified"], "2025-01-01T00:00:00Z");
    assert_eq!(entity["keywords"], json!(["Genomics"]));
    assert_eq!(
        entity["creator"][0]["identifier"][0],
        json!({
            "@type": "PropertyValue", "propertyID": "orcid", "value": "0000-0002-1825-0097"
        })
    );
    assert_eq!(
        entity["creator"][0]["affiliation"][0]["identifier"],
        "01ggx4157"
    );
}
