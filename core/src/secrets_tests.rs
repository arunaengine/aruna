//! Tests secret names, kinds, settings and field values.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const CANARY: &str = "canary-3f9a";

fn fields(pairs: &[(&str, &str)]) -> SecretFields {
    SecretFields::new(
        (pairs.iter())
            .map(|(name, value)| (name.to_string(), Secret::new(*value)))
            .collect(),
    )
}

fn kind(name: &str) -> SecretKind {
    SecretKind::parse(name).unwrap()
}

#[test]
fn names_follow_rule() {
    for name in ["catalog-key", "a", "0.token_v2", &"a".repeat(64)] {
        assert!(SecretName::try_from(name.to_string()).is_ok(), "{name}");
    }
    for name in [
        "",
        "-key",
        ".key",
        "Key",
        "key/x",
        "key key",
        &"a".repeat(65),
    ] {
        assert_eq!(
            SecretName::try_from(name.to_string()),
            Err(SecretError::Name)
        );
    }
    assert!(serde_json::from_str::<SecretName>("\"Bad\"").is_err());
}

#[test]
fn unknown_kind_refused() {
    assert_eq!(
        SecretKind::parse("plugin.x.y"),
        Err(SecretError::Kind("plugin.x.y".to_string()))
    );
    for name in ["bearer", "basic", "header", "hmac", "fields"] {
        assert_eq!(kind(name).spec().unwrap().name, name);
    }
}

#[test]
fn fields_match_kind() {
    assert_eq!(fields(&[("token", CANARY)]).check(&kind("bearer")), Ok(()));
    let basic = fields(&[("username", "u"), ("password", CANARY)]);
    assert_eq!(basic.check(&kind("basic")), Ok(()));
    let refused = [
        (fields(&[("token", CANARY), ("extra", "x")]), "bearer"),
        (fields(&[("token", "")]), "bearer"),
        (fields(&[("username", "u")]), "basic"),
        (fields(&[]), "fields"),
        (fields(&[("Bad", "x")]), "fields"),
    ];
    for (value, name) in refused {
        assert_eq!(value.check(&kind(name)), Err(SecretError::Fields), "{name}");
    }
    let many: Vec<_> = (0..=MAX_FIELDS).map(|index| format!("f{index}")).collect();
    let pairs: Vec<_> = many.iter().map(|name| (name.as_str(), "x")).collect();
    assert_eq!(
        fields(&pairs).check(&kind("fields")),
        Err(SecretError::Fields)
    );
    let large = "x".repeat(MAX_VALUE_BYTES);
    let value = fields(&[("blob", large.as_str())]);
    assert_eq!(value.check(&kind("fields")), Err(SecretError::TooLarge));
}

#[test]
fn encoding_is_deterministic() {
    let first = fields(&[("b", "2"), ("a", CANARY)]);
    let second = fields(&[("a", CANARY), ("b", "2")]);
    let bytes = first.encode().unwrap();
    assert_eq!(bytes.expose(), second.encode().unwrap().expose());
    assert_eq!(SecretFields::decode(&bytes).unwrap(), first);
    assert_eq!(
        SecretFields::decode(&SecretBytes::new(vec![0xff])),
        Err(SecretError::Encoding)
    );
}

#[test]
fn settings_follow_kind() {
    let header = kind("header");
    let with = |pairs: &[(&str, &str)]| -> SecretSettings {
        (pairs.iter())
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    };
    assert_eq!(
        check_settings(&header, &with(&[("header", "X-Api-Key")])),
        Ok(())
    );
    let refused = [
        (with(&[]), "header"),
        (with(&[("header", "Host")]), "header"),
        (with(&[("header", "bad header")]), "header"),
        (with(&[("header", "X-Key"), ("prefix", "x")]), "prefix"),
    ];
    for (settings, name) in refused {
        let error = check_settings(&header, &settings).unwrap_err();
        assert_eq!(error, SecretError::Setting(name.to_string()));
    }
    let hmac = kind("hmac");
    let signed = with(&[("header", "X-Signature"), ("prefix", "sha256=")]);
    assert_eq!(check_settings(&hmac, &signed), Ok(()));
}

#[test]
fn debug_hides_values() {
    let value = fields(&[("token", CANARY)]);
    let shown = format!("{value:?}");
    assert!(shown.contains("token") && !shown.contains(CANARY));
    let errors = [
        SecretError::Fields,
        SecretError::TooLarge,
        SecretError::Encoding,
    ];
    assert!(
        errors
            .iter()
            .all(|error| !error.to_string().contains(CANARY))
    );
}

fn plugin(install: u8) -> SecretConsumer {
    SecretConsumer::Plugin {
        plugin_id: "dataset-announcer".to_string(),
        install_id: Ulid::from_bytes([install; 16]),
    }
}

fn inject_rule(groups: Vec<GroupId>) -> UseRule {
    UseRule {
        consumer: plugin(1),
        delivery: Delivery::Inject,
        origins: vec!["https://api.example.org".to_string()],
        paths: vec!["/records".to_string()],
        methods: vec!["POST".to_string()],
        response: ResponseExposure::Status,
        groups,
    }
}

const GROUP: GroupId = Ulid::from_bytes([7; 16]);

fn request<'a>(consumer: &'a SecretConsumer, target: (&'a str, &'a Url)) -> UseRequest<'a> {
    UseRequest {
        consumer,
        delivery: Delivery::Inject,
        group: Some(GROUP),
        target: Some(target),
    }
}

#[test]
fn rules_checked_per_scope() {
    let bearer = kind("bearer");
    let group = SecretScope::Group(GROUP);
    assert_eq!(check_rules(group, &bearer, &[inject_rule(vec![])]), Ok(()));
    assert_eq!(
        check_rules(SecretScope::Realm, &bearer, &[inject_rule(vec![GROUP])]),
        Ok(())
    );
    let user = SecretScope::User(UserId::nil(
        crate::structs::identity::realm::RealmId::from_bytes([1; 32]),
    ));
    let mut http = inject_rule(vec![]);
    http.origins = vec!["http://api.example.org".to_string()];
    let mut with_path = inject_rule(vec![]);
    with_path.origins = vec!["https://api.example.org/v1".to_string()];
    let mut verify = inject_rule(vec![]);
    verify.delivery = Delivery::Verify;
    let refused = [
        (group, &bearer, inject_rule(vec![GROUP])),
        (SecretScope::Realm, &bearer, inject_rule(vec![])),
        (user, &bearer, inject_rule(vec![])),
        (group, &bearer, http),
        (group, &bearer, with_path),
        (group, &bearer, verify),
        (group, &kind("hmac"), inject_rule(vec![])),
    ];
    for (scope, kind, rule) in refused {
        assert_eq!(check_rules(scope, kind, &[rule]), Err(SecretError::Rule));
    }
}

#[test]
fn use_matches_rule() {
    let rules = [inject_rule(vec![])];
    let scope = SecretScope::Group(GROUP);
    let consumer = plugin(1);
    let url = Url::parse("https://api.example.org/records/42").unwrap();
    assert!(check_use(scope, &rules, &request(&consumer, ("POST", &url))).is_ok());
    let cases = [
        (
            "GET",
            "https://api.example.org/records/42",
            UseRefusal::NoRule,
        ),
        (
            "POST",
            "https://api.example.org/recordsx",
            UseRefusal::NoRule,
        ),
        (
            "POST",
            "https://other.example.org/records",
            UseRefusal::NoRule,
        ),
        (
            "POST",
            "https://api.example.org:8443/records",
            UseRefusal::NoRule,
        ),
        (
            "POST",
            "http://api.example.org/records",
            UseRefusal::PlainHttp,
        ),
    ];
    for (method, target, refusal) in cases {
        let url = Url::parse(target).unwrap();
        let result = check_use(scope, &rules, &request(&consumer, (method, &url)));
        assert_eq!(result.err(), Some(refusal), "{method} {target}");
    }
    let other = plugin(2);
    let result = check_use(scope, &rules, &request(&other, ("POST", &url)));
    assert_eq!(result.err(), Some(UseRefusal::NoRule));
}

#[test]
fn use_needs_group() {
    let consumer = plugin(1);
    let url = Url::parse("https://api.example.org/records").unwrap();
    let mut realm_install = request(&consumer, ("POST", &url));
    realm_install.group = None;
    let rules = [inject_rule(vec![])];
    let scope = SecretScope::Group(GROUP);
    assert_eq!(
        check_use(scope, &rules, &realm_install).err(),
        Some(UseRefusal::RealmInstall)
    );
    let foreign = SecretScope::Group(Ulid::from_bytes([8; 16]));
    let result = check_use(foreign, &rules, &request(&consumer, ("POST", &url)));
    assert_eq!(result.err(), Some(UseRefusal::OtherGroup));
    // A realm secret names the groups whose installs may use it.
    let shared = [inject_rule(vec![Ulid::from_bytes([8; 16])])];
    let result = check_use(
        SecretScope::Realm,
        &shared,
        &request(&consumer, ("POST", &url)),
    );
    assert_eq!(result.err(), Some(UseRefusal::NoRule));
    let allowed = [inject_rule(vec![GROUP])];
    assert!(
        check_use(
            SecretScope::Realm,
            &allowed,
            &request(&consumer, ("POST", &url))
        )
        .is_ok()
    );
}

#[test]
fn verify_needs_no_target() {
    let consumer = plugin(1);
    let rule = UseRule {
        delivery: Delivery::Verify,
        origins: vec![],
        paths: vec![],
        methods: vec![],
        ..inject_rule(vec![])
    };
    let use_request = UseRequest {
        consumer: &consumer,
        delivery: Delivery::Verify,
        group: Some(GROUP),
        target: None,
    };
    assert!(check_use(SecretScope::Group(GROUP), &[rule], &use_request).is_ok());
}
