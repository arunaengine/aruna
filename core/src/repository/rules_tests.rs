//! Checks the shipped mapping rules and the mapping preview.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::json;

use super::*;

fn bytes_id(seed: u8) -> String {
    W3idIdentifier::ContentHash([seed; 32]).to_w3id()
}

fn document(entities: Vec<Value>) -> Value {
    let mut graph = vec![
        json!({"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"}}),
        json!({"@id": "./", "@type": "Dataset", "name": "Data", "author": [{"@id": "#ada"}],
            "keywords": ["a"]}),
        json!({"@id": "#ada", "@type": "Person", "name": "Ada"}),
    ];
    graph.extend(entities);
    json!({"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": graph})
}

#[test]
fn shipped_rules_consistent() {
    assert!(rules(RepositoryConnectorKind::OaiPmh).unwrap().is_none());
    for descriptor in crate::repository::kinds() {
        let kind = descriptor.kind;
        let rules = descriptor.rules().expect("embedded rules parse");
        for endpoint in ["https://zenodo.org/api/", "https://repo.example/api/"] {
            let iri = (descriptor.profile)(endpoint);
            assert!(descriptor.profiles.iter().any(|profile| profile.iri == iri));
        }
        let mut names = rules
            .targets
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            rules.targets.len(),
            "{kind:?} target names repeat"
        );
        for target in &rules.targets {
            for relation in &target.relations {
                assert!(
                    rules.target(&relation.target).is_some(),
                    "{}",
                    relation.target
                );
            }
            if let Some(group) = &target.group {
                assert!(rules.target(&group.each).is_some(), "{}", group.each);
            }
            let mut fields = target
                .fields
                .iter()
                .map(|f| f.field.as_str())
                .collect::<Vec<_>>();
            fields.sort_unstable();
            fields.dedup();
            assert_eq!(
                fields.len(),
                target.fields.len(),
                "{} fields repeat",
                target.name
            );
            assert!(target.fields.iter().all(|field| !field.property.is_empty()));
        }
    }
    let invenio = rules(RepositoryConnectorKind::Invenio).unwrap().unwrap();
    assert!(invenio.target("record").is_some_and(|t| t.select.root));
    assert_eq!(invenio.target("file").unwrap().content.max_files, Some(100));
}

#[test]
fn unknown_rules_refused() {
    let unknown = json!({"targets": [{"name": "record", "select": {"root": true},
        "fields": [{"property": ["name"], "field": "title", "convert": "shout"}]}]});
    assert!(serde_json::from_value::<Rules>(unknown).is_err());
    let misspelled = json!({"targets": [{"name": "x", "select": {"roots": true}}]});
    assert!(serde_json::from_value::<Rules>(misspelled).is_err());
}

#[test]
fn preview_maps_invenio() {
    let rules = rules(RepositoryConnectorKind::Invenio).unwrap().unwrap();
    let data = bytes_id(1);
    let document = document(vec![
        json!({"@id": data, "@type": "File", "name": "data.csv"}),
        json!({"@id": "https://example.org/web.csv", "@type": "File"}),
    ]);
    let (mapped, findings) = preview(rules, &document);
    assert!(findings.is_empty(), "{findings:#?}");
    let entry = |id: &str, target: &str, field: Option<&str>| Mapped {
        entity_id: id.into(),
        target: target.into(),
        group: None,
        field: field.map(str::to_string),
    };
    assert!(mapped.contains(&entry("./", "record", None)));
    assert!(mapped.contains(&entry("./", "record", Some("title"))));
    assert!(mapped.contains(&entry("#ada", "record", Some("creators"))));
    assert!(mapped.contains(&entry(&data, "file", None)));
    // A web data entity stays a reference and the author no record file.
    assert!(
        !mapped
            .iter()
            .any(|m| m.entity_id == "https://example.org/web.csv")
    );
    assert!(
        !mapped
            .iter()
            .any(|m| m.entity_id == "#ada" && m.field.is_none())
    );
}

#[test]
fn preview_reports_violations() {
    let rules = rules(RepositoryConnectorKind::Invenio).unwrap().unwrap();
    let files = (0..101u8)
        .map(|seed| json!({"@id": bytes_id(seed), "@type": "File"}))
        .collect();
    let (_, findings) = preview(rules, &document(files));
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].code, "content_violation");
    assert_eq!(findings[0].rule, "file/max_files");

    let grouped: Rules = serde_json::from_value(json!({"targets": [
        {"name": "sample", "select": {"types": ["Sample"]}, "min": 1},
        {"name": "run", "select": {"types": ["File"]},
            "group": {"each": "sample", "property": "about"},
            "relations": [{"property": "about", "target": "sample"}]}
    ]}))
    .unwrap();
    let document = document(vec![
        json!({"@id": "#s1", "@type": "Sample"}),
        json!({"@id": "r1.fastq", "@type": "File", "about": {"@id": "#s1"}}),
        json!({"@id": "r2.fastq", "@type": "File"}),
    ]);
    let (mapped, findings) = preview(&grouped, &document);
    assert!(
        mapped
            .iter()
            .any(|m| m.entity_id == "r1.fastq" && m.group.as_deref() == Some("#s1"))
    );
    assert_eq!(findings.len(), 1, "{findings:#?}");
    assert_eq!(findings[0].code, "mapping_violation");
    assert_eq!(findings[0].focus_node.as_deref(), Some("r2.fastq"));
    let (_, findings) = preview(&grouped, &json!({"@graph": []}));
    assert_eq!(findings[0].rule, "sample/min");
}
