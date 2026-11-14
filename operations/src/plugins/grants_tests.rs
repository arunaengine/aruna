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

fn reader() -> Manifest {
    manifest(json!({ "permissions": { "required": ["metadata.read", "objects.read"] } }))
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
    let whole = BTreeSet::from([format!("{}/**", scope.group())]);
    assert_eq!(grants[&Grant::Read], whole);
    assert_eq!(grants[&Grant::Write], whole);
    let sends = &grants[&Grant::Services];
    assert_eq!(sends.len(), 2);
    assert!(sends.contains(&scope.connection("mirror")));
    let paths = role_paths(&grants);
    assert_eq!(role_name(&paths), WRITE_ROLE);
    assert_eq!(paths[&scope.connection("catalog")], Access::READ);

    let grants = default_grants(&reader(), &scope);
    assert!(!grants.contains_key(&Grant::Write));
    assert_eq!(role_name(&role_paths(&grants)), READ_ROLE);
    let objects = patterns_for(&grants, Permission::ObjectsRead);
    assert_eq!(objects, whole);
    assert!(patterns_for(&grants, Permission::MetadataCreate).is_empty());
}

#[test]
fn readable_secrets_granted() {
    let scope = scope();
    let manifest = manifest(json!({ "secrets": {
        "signing": { "title": "Signing key", "readable": true },
        "api": { "title": "API key" }
    } }));
    let grants = default_grants(&manifest, &scope);
    assert_eq!(
        grants[&Grant::Secrets],
        BTreeSet::from([scope.secret("signing")])
    );
}

#[test]
fn narrowing_rules() {
    let scope = scope();
    let manifest = manifest(json!({}));
    let meta = format!("{}/meta", scope.group());
    let dataset = format!("{meta}/runs/7/**");
    let data = format!("{}/data/results/**", scope.group());
    let narrowed = narrow_grants(
        &manifest,
        &scope,
        &choices(&[("write", &[]), ("read", &[&dataset, &data])]),
    )
    .unwrap();
    assert_eq!(
        narrowed[&Grant::Read],
        BTreeSet::from([data, dataset.clone()])
    );
    assert!(!narrowed.contains_key(&Grant::Write));
    assert_eq!(narrowed[&Grant::Services].len(), 2);
    assert_eq!(role_name(&role_paths(&narrowed)), READ_ROLE);

    let refusals = [
        (
            &manifest,
            choices(&[("connections.send", &["catalog"])]),
            GrantError::Unknown("connections.send".into()),
        ),
        (
            &manifest,
            choices(&[("metadata.read", &[])]),
            GrantError::Unknown("metadata.read".into()),
        ),
        (
            &reader(),
            choices(&[("write", &[&dataset])]),
            GrantError::NotRequested(Grant::Write),
        ),
        (
            &reader(),
            choices(&[("read", &[])]),
            GrantError::Required(Grant::Read),
        ),
    ];
    for (manifest, choice, expected) in refusals {
        assert_eq!(narrow_grants(manifest, &scope, &choice), Err(expected));
    }
    let other_group = format!("/{}/g/{}/meta/**", scope.realm_id, Ulid::from_parts(5, 5));
    for pattern in [
        other_group,
        format!("{}/plugins/**", scope.group()),
        format!("{meta}/../admin"),
        format!("{meta}/a*/b"),
        format!("{meta}/**/x"),
        meta.clone(),
    ] {
        let result = narrow_grants(&manifest, &scope, &choices(&[("read", &[&pattern])]));
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
    let dataset = format!("{}/meta/runs/7/**", scope.group());
    let granted = narrow_grants(&old, &scope, &choices(&[("write", &[&dataset])])).unwrap();
    let new = manifest(json!({
        "connections": { "catalog": { "url": "https://catalog.example.org/api/" }, "archive": { "url": "https://a.example.org/" } }
    }));
    let (kept, missing) = carry_grants(&granted, &new, &scope);
    assert_eq!(kept[&Grant::Write], BTreeSet::from([dataset]));
    assert_eq!(kept[&Grant::Read], granted[&Grant::Read]);
    let services = [scope.connection("archive"), scope.connection("catalog")];
    assert_eq!(kept[&Grant::Services], BTreeSet::from(services));
    assert!(missing.is_empty());
    assert_eq!(parse_grants(&stored_grants(&kept)), kept);

    // A read-only version drops write; one that requires writing again needs a new grant.
    let (kept, _) = carry_grants(&granted, &reader(), &scope);
    assert!(!kept.contains_key(&Grant::Write));
    let writer = manifest(json!({ "permissions": { "required": ["metadata.write"] } }));
    let (_, missing) = carry_grants(&kept, &writer, &scope);
    assert_eq!(missing, vec!["write".to_string()]);
}
