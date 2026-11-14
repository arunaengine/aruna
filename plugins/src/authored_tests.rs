//! Tests the plugin.toml reader: short forms, defaults, and the errors an author sees.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const SHORT: &str = r#"
name = "map-reads"
version = "0.1.0"
on = ["manual", "timer", "webhook", "dataset_created"]
access = "write"

[settings]
results = "Bucket path for results"
chat_url = { type = "url" }

[services.ncbi]
url = "https://api.ncbi.nlm.nih.gov/datasets"
secret = "NCBI API key"

[services.public]
url = "{chat_url}/api"
methods = ["GET"]

[secrets.signing]
title = "Signing key"
kind = "basic"

[timer]
every = "1d"

[webhook]
secret = "Webhook key"
delivery_id = "x-delivery"

[manual]
dataset = { type = "dataset" }
reference = "Reference genome accession"
"#;

#[test]
fn short_forms() {
    let manifest = parse(SHORT).unwrap();
    assert_eq!(manifest.check(), Ok(()));
    let results = &manifest.settings["results"];
    assert_eq!(
        (results.title.as_str(), results.kind),
        ("Bucket path for results", SettingKind::String)
    );
    assert_eq!(manifest.settings["chat_url"].title, "Chat url");
    let ncbi = &manifest.connections["ncbi"];
    assert_eq!(ncbi.auth.as_deref(), Some("ncbi"));
    assert_eq!(ncbi.methods, all_methods());
    assert_eq!(manifest.secrets["ncbi"].kind, SecretKind::Bearer);
    assert_eq!(manifest.connections["public"].auth, None);
    assert_eq!(manifest.secrets["signing"].kind, SecretKind::Basic);
    assert!(manifest.secrets["signing"].readable && !manifest.secrets["ncbi"].readable);
    let hook = &manifest.triggers[&TriggerKind::Webhook];
    assert_eq!(hook.verify.as_deref(), Some("webhook"));
    assert_eq!(manifest.secrets["webhook"].kind, SecretKind::Hmac);
    assert_eq!(
        manifest.triggers[&TriggerKind::Every].interval.as_deref(),
        Some("1d")
    );
    let form = &manifest.triggers[&TriggerKind::Manual].form;
    assert_eq!(form["dataset"].kind, SettingKind::Dataset);
    assert_eq!(form["reference"].kind, SettingKind::String);
    let required = &manifest.permissions.required;
    for permission in [Permission::MetadataWrite, Permission::SecretsValue] {
        assert!(required.contains(&permission), "{permission}");
    }
}

#[test]
fn read_by_default() {
    let manifest =
        parse("name = \"p\"\nversion = \"0.1.0\"\non = [\"dataset_created\"]\n").unwrap();
    assert_eq!(manifest.check(), Ok(()));
    let required = &manifest.permissions.required;
    assert!(required.contains(&Permission::MetadataRead));
    assert!(!required.contains(&Permission::MetadataWrite));
    assert_eq!(manifest.title, None);
}

#[test]
fn author_errors() {
    let base = "name = \"p\"\nversion = \"0.1.0\"\n";
    let error = |rest: &str| match parse(&format!("{base}{rest}")) {
        Err(ManifestError::Malformed(message)) => message,
        other => panic!("{rest}: {other:?}"),
    };
    let cases = [
        ("on = [\"timer\"]\n", "there is no `[timer]` with `every`"),
        (
            "on = [\"webhook\"]\n",
            "there is no `[webhook]` with `secret`",
        ),
        ("on = [\"tick\"]\n", "`tick`, which is no trigger"),
        ("on = [\"manual\", \"manual\"]\n", "lists `manual` twice"),
        (
            "on = [\"manual\"]\n[timer]\nevery = \"1h\"\n",
            "`on` does not list `timer`",
        ),
        (
            "on = [\"timer\"]\n[timer]\nevery = \"1h\"\n[manual]\na = \"A\"\n",
            "does not list `manual`",
        ),
        (
            "on = [\"manual\"]\n[services.webhook]\nurl = \"https://x.example\"\nsecret = \"S\"\n[secrets.webhook]\ntitle = \"T\"\n",
            "`webhook` names two secrets",
        ),
        ("on = [\"manual\"]\ntitel = \"x\"\n", "unknown field"),
        ("on = [\"manual\"]\n[build]\noutput = \"x\"\n", "unknown field `build`"),
    ];
    for (rest, expected) in cases {
        let message = error(rest);
        assert!(message.contains(expected), "{rest}: {message}");
    }
}

#[test]
fn form_and_setting_types() {
    let picked = "name = \"p\"\nversion = \"0.1.0\"\non = [\"manual\"]\n[settings]\nd = { type = \"dataset\" }\n";
    assert!(matches!(
        parse(picked).unwrap().check(),
        Err(ManifestError::InvalidSetting(name, _)) if name == "d"
    ));
}
