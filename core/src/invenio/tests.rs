//! Checks repository identity and preservation of version-specific file references.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

#[test]
fn binds_repository_login() {
    use crate::credential_encryption::CredentialEncryptionKey;
    use crate::structs::execution::job::{ExportRoCrateSpec, JobPayload, RoCrateLimits};
    use crate::structs::identity::auth::AuthContext;
    use crate::structs::identity::realm::RealmId;
    let key = CredentialEncryptionKey::derive(&[1; 32]);
    let user = crate::UserId::local(Ulid::from_bytes([2; 16]), RealmId::from_bytes([3; 32]));
    let group = Ulid::from_bytes([4; 16]);
    let connector = Ulid::from_bytes([5; 16]);
    let endpoint = "https://zenodo.org/api/";
    let credential = InvenioCredential::seal(
        &key,
        user,
        group,
        connector,
        endpoint.into(),
        "author-token",
    )
    .unwrap();
    assert_eq!(
        credential
            .open(&key, user, group, connector, endpoint)
            .unwrap(),
        "author-token"
    );
    assert!(
        credential
            .open(
                &key,
                crate::UserId::nil(user.realm_id),
                group,
                connector,
                endpoint
            )
            .is_err()
    );
    assert!(
        credential
            .open(
                &CredentialEncryptionKey::derive(&[9; 32]),
                user,
                group,
                connector,
                endpoint
            )
            .is_err()
    );
    assert!(
        credential
            .open(&key, user, group, connector, "https://other.example/api/")
            .is_err()
    );
    assert!(!format!("{credential:?}").contains("author-token"));
    assert!(
        !serde_json::to_string(&credential)
            .unwrap()
            .contains("author-token")
    );
    let payload = |token| {
        JobPayload::ExportRoCrate(ExportRoCrateSpec {
            auth_context: AuthContext {
                user_id: user,
                realm_id: user.realm_id,
                path_restrictions: None,
                session: None,
            },
            document_id: Ulid::nil(),
            limits: RoCrateLimits::default(),
            destination: Some(InvenioDestination {
                group_id: group,
                connector_id: connector,
                draft_id: None,
                new_version: None,
                metadata_json: "{}".into(),
                publish: true,
                public_files: false,
                credential: Some(
                    InvenioCredential::seal(&key, user, group, connector, endpoint.into(), token)
                        .unwrap(),
                ),
            }),
        })
    };
    assert_eq!(
        payload("author-token").plan_digest(),
        payload("author-token").plan_digest()
    );
    assert_ne!(
        payload("author-token").plan_digest(),
        payload("different-author").plan_digest()
    );
}

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
        assert_eq!(
            version["isBasedOn"]["@id"],
            format!("https://zenodo.org/api/records/{id}")
        );
        assert_eq!(version["hasPart"].as_array().unwrap().len(), 2);
    }
    craqle::validate_rocrate_jsonld(&document.to_string()).unwrap();
}

#[test]
fn requires_selected_version() {
    assert!(import_crate("https://example.org/api/", "9", &[]).is_err());
}

#[test]
fn preserves_date_precision() {
    for (date, start) in [
        ("2020", "2020-01-01"),
        ("2020-11", "2020-11-01"),
        ("2020-11-10", "2020-11-10"),
        ("1939/1945", "1939-01-01"),
        ("1939-09-01/1945-09", "1939-09-01"),
    ] {
        let record = json!({"id": "1", "metadata": {
            "title": "Dates", "publication_date": date,
            "creators": [{"person_or_org": {"type": "personal", "family_name": "Researcher"}}]
        }});
        let document = import_crate(
            "https://zenodo.org/api/",
            "1",
            &[(record, json!({"entries": []}))],
        )
        .unwrap();
        craqle::validate_rocrate_jsonld(&document.to_string()).unwrap();
        let root = document["@graph"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["@id"] == "./")
            .unwrap();
        assert_eq!(root["datePublished"], start);
        assert_eq!(root[PUBLICATION_DATE], date);
        assert_eq!(
            export_metadata(&document, &Value::Null).unwrap()["publication_date"],
            date
        );
    }
    for date in ["", "2020-13", "2021-02-29", "2020/x", "2020/2021/2022"] {
        assert!(publication_start(date).is_err(), "{date}");
    }
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

#[test]
fn derives_export_metadata() {
    let document = json!({"@graph": [
        {"@id": "ro-crate-metadata.json", "about": {"@id": "./"}},
        {"@id": "./", "name": "Source title", "description": "Source description",
            "datePublished": "2024-01-01T12:00:00Z", "creator": {"@id": "#author"},
            "identifier": "https://doi.org/10.1234/source", "keywords": ["genomics"],
            "license": {"@id": "https://example.org/license"}},
        {"@id": "#author", "@type": "Person", "name": "Researcher, A", "familyName": "Researcher",
            "givenName": "A", "identifier": {"@type": "PropertyValue", "propertyID": "orcid", "value": "0000-0002-1825-0097"}}
    ]});
    let metadata = export_metadata(&document, &json!({"title": "Chosen title"})).unwrap();
    assert_eq!(metadata["title"], "Chosen title");
    assert_eq!(metadata["description"], "Source description");
    assert_eq!(metadata["publication_date"], "2024-01-01");
    assert_eq!(
        metadata["creators"][0]["person_or_org"]["identifiers"][0]["identifier"],
        "0000-0002-1825-0097"
    );
    assert_eq!(
        metadata["related_identifiers"][0],
        json!({"scheme": "doi", "identifier": "10.1234/source", "relation_type": {"id": "isderivedfrom"}})
    );
    assert_eq!(metadata["subjects"], json!([{"subject": "genomics"}]));
    assert_eq!(metadata["rights"][0]["link"], "https://example.org/license");
    assert!(export_metadata(&json!({}), &json!({})).is_err());
}

#[test]
fn retains_native_fields() {
    let metadata = json!({"title": "Software", "publication_date": "2020-11", "resource_type": {"id": "software"},
        "creators": [{"person_or_org": {"type": "personal", "name": "Researcher, A", "family_name": "Researcher"},
            "affiliations": [{"id": "01ggx4157", "name": "CERN"}], "role": {"id": "researcher"}}],
        "funding": [{"funder": {"id": "01ggx4157", "name": "CERN"}, "award": {"number": "12345"}}],
        "rights": [{"id": "cc-by-4.0"}],
        "related_identifiers": [{"identifier": "10.1234/paper", "scheme": "doi", "relation_type": {"id": "issupplementto"}}]
    });
    let fields = json!({"local:experiment": "sequencing"});
    let document = import_crate(
        "https://zenodo.org/api/",
        "1",
        &[(
            json!({"id": "1", "metadata": metadata, "custom_fields": fields}),
            json!({"entries": []}),
        )],
    )
    .unwrap();
    let validated = craqle::validate_rocrate_jsonld(&document.to_string()).unwrap();
    assert!(validated.nquads.contains("metadata/funding/0/award/number"));
    assert!(validated.nquads.contains("12345"));
    let exported = export_fields(&document, &Value::Null).unwrap();
    assert_eq!(exported["metadata"], metadata);
    assert_eq!(exported["custom_fields"], fields);
    assert_eq!(
        export_fields(&document, &json!({"title": "Edited"})).unwrap()["metadata"]["title"],
        "Edited"
    );
}
