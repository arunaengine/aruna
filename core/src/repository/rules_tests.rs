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
    // Rules the preview and export would not evaluate never load.
    for (rules, reason) in [
        (
            json!({"targets": [{"name": "run", "select": {}, "content": {"md5": true}}]}),
            "md5",
        ),
        (
            json!({"targets": [{"name": "run", "select": {}, "content": {"format": "vcf"}}]}),
            "format vcf",
        ),
        (
            json!({"targets": [{"name": "run", "select": {},
                "group": {"each": "sample", "property": "about"}}]}),
            "target sample",
        ),
        (
            json!({"targets": [{"name": "run", "select": {},
                "relations": [{"property": "about", "target": "run", "min": 2, "max": 1}]}]}),
            "max below min",
        ),
        (
            json!({"targets": [{"name": "run", "select": {}}, {"name": "run", "select": {}}]}),
            "repeats",
        ),
    ] {
        let error = load(&rules.to_string()).expect_err(reason);
        assert!(
            error.contains(reason.split(' ').next_back().unwrap()),
            "{error}"
        );
    }
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn formats_read_prefix() {
    let bam = gzip(b"BAM\x01rest of the header");
    let fastq = gzip(b"@read1\nACGT\n+\n!!!!\n");
    assert!(has_format("fastq", b"@read1\nACGT"));
    assert!(has_format("fastq", &fastq));
    // A truncated prefix of a larger member still shows its first bytes.
    assert!(has_format("bam", &bam[..bam.len() - 4]));
    assert!(!has_format("bam", b"BAM\x01"));
    assert!(has_format("cram", b"CRAM\x03\x00"));
    assert!(!has_format("cram", &gzip(b"CRAM")));
    assert!(!has_format("fastq", b">fasta"));
    assert!(!has_format("vcf", b"##fileformat"));
}

#[test]
fn content_rules_checked() {
    let target: Target = serde_json::from_value(json!({"name": "run", "select": {},
        "content": {"max_files": 2, "max_file_bytes": 10, "max_total_bytes": 15,
            "format": "fastq"}}))
    .unwrap();
    let file = |path, size, prefix| FileFacts {
        path,
        size: Some(size),
        prefix: Some(prefix),
    };
    let ok = [file("a_1.fastq", 8, b"@a"), file("a_2.fastq", 7, b"@b")];
    assert!(content_findings(&target, &ok).is_empty());
    let bad = [
        file("a.fastq", 11, b"@a"),
        file("b.fastq", 5, b">b"),
        file("c.fastq", 1, b"@c"),
    ];
    let rules = content_findings(&target, &bad)
        .into_iter()
        .map(|finding| (finding.rule, finding.focus_node.unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(
        rules,
        [
            ("run/max_files".to_string(), "./".to_string()),
            ("run/max_file_bytes".into(), "a.fastq".into()),
            ("run/format".into(), "b.fastq".into()),
            ("run/max_total_bytes".into(), "./".into()),
        ]
    );
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
    // Every export also uploads the crate metadata and its report as files.
    assert!(mapped.contains(&entry("ro-crate-metadata.json", "file", None)));
    assert_eq!(target_files("file", &mapped, |id| Some(id.into())).len(), 3);
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
fn creators_take_union() {
    let rules = rules(RepositoryConnectorKind::Invenio).unwrap().unwrap();
    let mut document = document(vec![
        json!({"@id": "#bob", "@type": "Person", "name": "Bob"}),
    ]);
    document["@graph"][1]["creator"] = json!([{"@id": "#bob"}, {"@id": "#ada"}]);
    let (mapped, _) = preview(rules, &document);
    let creators = mapped
        .iter()
        .filter(|m| m.field.as_deref() == Some("creators"))
        .map(|m| m.entity_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(creators, ["#bob", "#ada"]);
}

#[test]
fn preview_reports_violations() {
    let rules = rules(RepositoryConnectorKind::Invenio).unwrap().unwrap();
    // With the crate metadata and report, 99 data files already make 101 uploads.
    let files = (0..99u8)
        .map(|seed| json!({"@id": bytes_id(seed), "@type": "File"}))
        .collect();
    let (_, findings) = preview(rules, &document(files));
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].code, "content_violation");
    assert_eq!(findings[0].rule, "file/max_files");

    let grouped: Rules = serde_json::from_value(json!({"targets": [
        {"name": "sample", "select": {"types": ["Sample"]}, "min": 1},
        {"name": "run", "select": {"types": ["File"]},
            "group": {"each": "sample", "property": "about", "pair": ["_1", "_2"]},
            "relations": [{"property": "about", "target": "sample", "max": 1}]},
        {"name": "study", "select": {"types": ["Study"]},
            "relations": [{"property": "isPartOf", "target": "sample", "min": 1,
                "inverse": true}]}
    ]}))
    .unwrap();
    let document = document(vec![
        json!({"@id": "#s1", "@type": "Sample", "isPartOf": {"@id": "#study"}}),
        json!({"@id": "#s2", "@type": "Sample"}),
        json!({"@id": "#study", "@type": "Study"}),
        json!({"@id": "#lonely", "@type": "Study"}),
        json!({"@id": "r_1.fastq", "@type": "File", "about": {"@id": "#s1"}}),
        json!({"@id": "r_2.fastq", "@type": "File", "about": {"@id": "#s1"}}),
        json!({"@id": "x_1.fastq", "@type": "File", "about": [{"@id": "#s1"}, {"@id": "#s2"}]}),
        json!({"@id": "loose.fastq", "@type": "File"}),
    ]);
    let (mapped, findings) = preview(&grouped, &document);
    assert!(
        mapped
            .iter()
            .any(|m| m.entity_id == "r_1.fastq" && m.group.as_deref() == Some("#s1"))
    );
    let mut found = findings
        .iter()
        .map(|finding| {
            assert_eq!(finding.code, "mapping_violation");
            (
                finding.rule.as_str(),
                finding.focus_node.as_deref().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    found.sort_unstable();
    assert_eq!(
        found,
        [
            ("run/group", "loose.fastq"),
            ("run/pair", "x_1.fastq"),
            ("run/relation", "loose.fastq"),
            ("run/relation", "x_1.fastq"),
            ("study/relation", "#lonely"),
        ]
    );
    let (_, findings) = preview(&grouped, &json!({"@graph": []}));
    assert_eq!(findings[0].rule, "sample/min");
}
