//! Tests default grants, narrowing inside the group, the computed role and update carry-over.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use serde_json::json;

fn manifest(extra: serde_json::Value) -> Manifest {
    let mut value = json!({
        "name": "dataset-announcer",
        "version": "0.1.0",
        "permissions": { "required": ["metadata.read"], "optional": ["metadata.write"] },
        "connections": {
            "catalog": { "url": "https://catalog.example.org/api/" },
            "mirror": { "url": "https://mirror.example.org/", "optional": true }
        }
    });
    if let (Some(base), Some(extra)) = (value.as_object_mut(), extra.as_object()) {
        base.extend(extra.clone());
    }
    serde_json::from_value(value).unwrap()
}

fn scope() -> InstallScope {
    InstallScope {
        realm_id: RealmId::from_bytes([1u8; 32]),
        group_id: Ulid::from_parts(1, 9),
        install_id: Ulid::from_parts(1, 7),
    }
}

fn choices(pairs: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    (pairs.iter())
        .map(|(name, patterns)| {
            (
                name.to_string(),
                patterns.iter().map(|p| p.to_string()).collect(),
            )
        })
        .collect()
}

#[test]
fn defaults_cover_group() {
    let scope = scope();
    let grants = default_grants(&manifest(json!({})), &scope);
    let meta = format!("/{}/g/{}/meta/**", scope.realm_id, scope.group_id);
    assert_eq!(
        grants[&Permission::MetadataRead],
        BTreeSet::from([meta.clone()])
    );
    assert_eq!(grants[&Permission::MetadataWrite], BTreeSet::from([meta]));
    let sends = &grants[&Permission::ConnectionsSend];
    assert_eq!(sends.len(), 2);
    assert!(sends.contains(&scope.connection("mirror")));
    let paths = role_paths(&grants);
    assert_eq!(role_name(&paths), WRITE_ROLE);
    assert_eq!(paths[&scope.connection("catalog")], Access::READ);
}

#[test]
fn narrowing_rules() {
    let scope = scope();
    let manifest = manifest(json!({}));
    let meta = scope.root(Permission::MetadataRead);
    let dataset = format!("{meta}/runs/7/**");
    let narrowed = narrow_grants(
        &manifest,
        &scope,
        &choices(&[
            ("metadata.write", &[]),
            ("metadata.read", &[&dataset]),
            ("connections.send", &["catalog"]),
        ]),
    )
    .unwrap();
    assert_eq!(
        narrowed[&Permission::MetadataRead],
        BTreeSet::from([dataset])
    );
    assert!(!narrowed.contains_key(&Permission::MetadataWrite));
    assert_eq!(role_name(&role_paths(&narrowed)), READ_ROLE);

    let other_group = format!("/{}/g/{}/meta/**", scope.realm_id, Ulid::from_parts(5, 5));
    let refusals = [
        (
            choices(&[("metadata.read", &[])]),
            GrantError::Required("metadata.read".into()),
        ),
        (
            choices(&[("objects.read", &["x"])]),
            GrantError::NotRequested(Permission::ObjectsRead),
        ),
        (
            choices(&[("metadata.delete", &[])]),
            GrantError::Unknown("metadata.delete".into()),
        ),
        (
            choices(&[("connections.send", &["mirror"])]),
            GrantError::Required("connection `catalog`".into()),
        ),
        (
            choices(&[("connections.send", &["catalog", "evil"])]),
            GrantError::UnknownConnection("evil".into()),
        ),
    ];
    for (choice, expected) in refusals {
        assert_eq!(narrow_grants(&manifest, &scope, &choice), Err(expected));
    }
    for pattern in [
        other_group,
        format!("{meta}/../admin"),
        format!("{meta}/a*/b"),
        format!("{meta}/**/x"),
        meta.clone(),
    ] {
        let result = narrow_grants(
            &manifest,
            &scope,
            &choices(&[("metadata.read", &[&pattern])]),
        );
        assert!(
            matches!(result, Err(GrantError::Outside { .. })),
            "{pattern}: {result:?}"
        );
    }
}

#[test]
fn unsupported_manifests() {
    let cases = [
        json!({}),
        json!({ "secrets": { "key": { "title": "Key" } } }),
        json!({ "triggers": { "webhook": {} } }),
        json!({ "permissions": { "optional": ["secrets.value"] } }),
    ];
    for extra in cases {
        assert_eq!(check_supported(&manifest(extra.clone())), Ok(()), "{extra}");
    }
    let jobs = manifest(json!({ "permissions": { "optional": ["jobs.run"] } }));
    assert!(check_supported(&jobs).is_err());
}

#[test]
fn update_keeps_grants() {
    let scope = scope();
    let old = manifest(json!({}));
    let dataset = format!("{}/runs/7/**", scope.root(Permission::MetadataRead));
    let granted =
        narrow_grants(&old, &scope, &choices(&[("metadata.write", &[&dataset])])).unwrap();
    let new = manifest(json!({
        "permissions": { "required": ["metadata.read", "objects.read"], "optional": ["metadata.write"] },
        "connections": { "catalog": { "url": "https://catalog.example.org/api/" }, "archive": { "url": "https://a.example.org/" } }
    }));
    let (kept, missing) = carry_grants(&granted, &new, &scope);
    assert_eq!(kept[&Permission::MetadataWrite], BTreeSet::from([dataset]));
    assert!(!kept.contains_key(&Permission::ObjectsRead));
    assert_eq!(
        kept[&Permission::ConnectionsSend],
        BTreeSet::from([scope.connection("catalog")])
    );
    assert_eq!(
        missing,
        vec![
            "objects.read".to_string(),
            "connection `archive`".to_string()
        ]
    );
    assert_eq!(parse_grants(&stored_grants(&kept)), kept);
}
