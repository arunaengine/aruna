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
