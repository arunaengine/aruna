//! Checks the shared crate field readers on prefixed keys, references and identifier forms.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use serde_json::json;

fn id(scheme: &str, value: &str) -> Identifier {
    Identifier {
        scheme: scheme.into(),
        value: value.into(),
    }
}

#[test]
fn finds_crate_root() {
    let described = json!({"@graph": [
        {"@id": "./", "name": "fallback"},
        {"@id": "data/ro-crate-metadata.json", "schema:about": {"@id": "data/"}},
        {"@id": "data/", "name": "described"}
    ]});
    assert_eq!(crate_root(&described).unwrap()["name"], "described");
    let plain = json!({"@graph": [{"@id": "./", "name": "fallback"}]});
    assert_eq!(crate_root(&plain).unwrap()["name"], "fallback");
    assert!(crate_root(&json!({"@graph": [{"@id": "other"}]})).is_none());
    assert!(crate_root(&json!({})).is_none());
}

#[test]
fn reads_prefixed_keys() {
    let entity = json!({
        "http://schema.org/keywords": ["a", 1, "b"],
        "https://schema.org/license": [{"@id": "https://spdx.org/licenses/MIT"}, "CC0-1.0", 2]
    });
    assert_eq!(keywords(&entity), ["a", "b"]);
    assert_eq!(
        licenses(&entity),
        ["https://spdx.org/licenses/MIT", "CC0-1.0"]
    );
    assert!(schema_value(&entity, "name").is_null());
    assert!(values(&Value::Null).is_empty());
    assert_eq!(values(&json!("one")), [json!("one")]);
}

#[test]
fn resolves_person_references() {
    let graph = [
        json!({"@id": "#org", "@type": ["Thing", "http://schema.org/Organization"],
            "name": "Institute", "identifier": "https://ror.org/01ggx4157/"}),
        json!({"@id": "#nameless", "@type": "Organization"}),
        json!({"@id": "#ada", "@type": "Person", "givenName": "Ada", "familyName": "Lovelace",
            "identifier": [
                {"@id": "https://orcid.org/0000-0002-1825-0097"},
                {"propertyID": "GND", "value": "118640445"},
                {"propertyID": "email", "value": "ada@example.org"},
                "https://example.org/profile"
            ],
            "affiliation": [{"@id": "#org"}, {"@id": "#nameless"}, {"name": "Inline"}]}),
    ];
    let ada = person(&graph, &json!({"@id": "#ada"}));
    assert!(!ada.organization);
    assert_eq!(
        (ada.given_name.as_deref(), ada.family_name.as_deref()),
        (Some("Ada"), Some("Lovelace"))
    );
    assert_eq!(
        ada.identifiers,
        [id("orcid", "0000-0002-1825-0097"), id("gnd", "118640445")]
    );
    assert_eq!(ada.affiliations, ["Institute", "Inline"]);
    let org = person(&graph, &graph[0]);
    assert!(org.organization);
    assert_eq!(org.identifiers, [id("ror", "01ggx4157")]);
    assert_eq!(
        person(&graph, &json!({"@id": "#missing"})),
        Person::default()
    );
}

#[test]
fn detects_identifier_schemes() {
    let cases = [
        (
            json!("https://doi.org/10.1234/a"),
            Some(id("doi", "10.1234/a")),
        ),
        (
            json!({"@id": "http://doi.org/10.1234/b"}),
            Some(id("doi", "10.1234/b")),
        ),
        (
            json!({"schema:propertyID": "ark", "schema:value": "ark:/1/2"}),
            Some(id("ark", "ark:/1/2")),
        ),
        (
            json!("https://example.org/x"),
            Some(id("url", "https://example.org/x")),
        ),
        (json!("plain text"), None),
        (json!({"propertyID": "doi"}), None),
    ];
    for (value, expected) in cases {
        assert_eq!(identifier(&value), expected, "{value}");
    }
    let isni = json!("http://isni.org/isni/0000000121032683");
    assert_eq!(
        person_identifier(&isni),
        Some(id("isni", "0000000121032683"))
    );
    assert_eq!(
        person_identifier(&json!({"propertyID": "ORCID", "value": "0000-0001"})),
        Some(id("orcid", "0000-0001"))
    );
    assert_eq!(person_identifier(&json!("https://doi.org/10.1/x")), None);
}
