//! Tests manifest checks, trigger wiring and the import rule on the dataset announcer example.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const EXAMPLE: &str = r#"
name = "dataset-announcer"
version = "0.1.0"
title = "Announce new datasets"

[settings]
catalog_url = { title = "Catalog address", type = "url" }

[secrets]
catalog_key = { title = "Catalog API key", kind = "bearer" }

[connections.catalog]
url = "{catalog_url}/api/records"
methods = ["POST"]
auth = "catalog_key"
response = ["/id"]

[triggers]
dataset_created = {}
manual = { handler = "dataset_created", input = "dataset" }

[permissions]
required = ["metadata.read"]
optional = ["metadata.write"]
"#;

fn example() -> Manifest {
    toml::from_str(EXAMPLE).unwrap()
}

fn handlers(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| name.to_string()).collect()
}

#[test]
fn example_wires() {
    let mut manifest = example();
    assert_eq!(manifest.check(), Ok(()));
    manifest.wire(handlers(&["dataset_created"])).unwrap();
    assert_eq!(manifest.abi.as_deref(), Some(PLUGIN_ABI));
    let created = &manifest.triggers[&TriggerKind::DatasetCreated];
    assert_eq!(created.handler.as_deref(), Some("dataset_created"));
    let json = serde_json::to_vec(&manifest).unwrap();
    assert_eq!(Manifest::parse(&json).unwrap(), manifest);
}

#[test]
fn wiring_defaults() {
    let mut only = example();
    only.triggers.get_mut(&TriggerKind::Manual).unwrap().handler = None;
    only.wire(handlers(&["announce"])).unwrap();
    let manual = &only.triggers[&TriggerKind::Manual];
    assert_eq!(manual.handler.as_deref(), Some("announce"));

    let mut missing = example();
    let result = missing.wire(handlers(&["announce", "resync"]));
    assert_eq!(
        result,
        Err(ManifestError::NoHandler(TriggerKind::DatasetCreated))
    );

    let mut unused = example();
    let result = unused.wire(handlers(&["dataset_created", "resync"]));
    assert_eq!(result, Err(ManifestError::UnusedHandler("resync".into())));

    let mut unknown = example();
    unknown
        .triggers
        .get_mut(&TriggerKind::Manual)
        .unwrap()
        .handler = Some("resync".into());
    let result = unknown.wire(handlers(&["dataset_created"]));
    assert_eq!(result, Err(ManifestError::UnknownHandler("resync".into())));
}

#[test]
fn rejects_requests() {
    let mut manifest = example();
    manifest.permissions.required.clear();
    assert_eq!(
        manifest.check(),
        Err(ManifestError::MissingPermission(
            TriggerKind::DatasetCreated,
            Permission::MetadataRead
        ))
    );
    let mut twice = example();
    twice.permissions.optional.insert(Permission::MetadataRead);
    assert_eq!(
        twice.check(),
        Err(ManifestError::InvalidPermission(Permission::MetadataRead))
    );
    let mut generated = example();
    generated
        .permissions
        .optional
        .insert(Permission::ConnectionsSend);
    assert_eq!(
        generated.check(),
        Err(ManifestError::InvalidPermission(
            Permission::ConnectionsSend
        ))
    );
    let typo = EXAMPLE.replace("metadata.write", "metadata.delete");
    assert!(toml::from_str::<Manifest>(&typo).is_err());
    let unknown_key = EXAMPLE.replace("title = \"Announce", "titel = \"Announce");
    assert!(toml::from_str::<Manifest>(&unknown_key).is_err());
}

#[test]
fn rejects_connections() {
    let mut fixed = example();
    let catalog = fixed.connections.get_mut("catalog").unwrap();
    catalog.url = "https://catalog.example.org/api".into();
    assert_eq!(fixed.check(), Ok(()));

    let mut not_url = example();
    not_url.settings.get_mut("catalog_url").unwrap().kind = SettingKind::String;
    assert!(matches!(
        not_url.check(),
        Err(ManifestError::InvalidConnection(name, _)) if name == "catalog"
    ));

    let mut hmac = example();
    hmac.secrets.get_mut("catalog_key").unwrap().kind = SecretKind::Hmac;
    assert_eq!(
        hmac.check(),
        Err(ManifestError::InvalidSlot("catalog_key".into()))
    );

    let mut no_slot = example();
    no_slot.secrets.clear();
    assert_eq!(
        no_slot.check(),
        Err(ManifestError::InvalidSlot("catalog_key".into()))
    );
}

#[test]
fn rejects_triggers() {
    let with = |kind: TriggerKind, trigger: Trigger| {
        let mut manifest = example();
        manifest.triggers.insert(kind, trigger);
        manifest.check()
    };
    let interval = |text: &str| Trigger {
        interval: Some(text.into()),
        ..Trigger::default()
    };
    assert_eq!(with(TriggerKind::Every, interval("1h")), Ok(()));
    for text in ["30s", "0m", "h", "367d", "1.5h"] {
        assert!(with(TriggerKind::Every, interval(text)).is_err(), "{text}");
    }
    assert!(with(TriggerKind::Manual, interval("1h")).is_err());
    assert!(with(TriggerKind::Webhook, Trigger::default()).is_err());
    let hook = |slot: &str, header: Option<&str>| Trigger {
        verify: Some(slot.into()),
        delivery_id: header.map(str::to_string),
        ..Trigger::default()
    };
    let mut verified = example();
    verified.secrets.insert(
        "hook_key".into(),
        SecretSlot {
            title: "Webhook key".into(),
            kind: SecretKind::Hmac,
        },
    );
    verified
        .triggers
        .insert(TriggerKind::Webhook, hook("hook_key", Some("x-delivery")));
    assert_eq!(verified.check(), Ok(()));
    verified
        .triggers
        .insert(TriggerKind::Webhook, hook("hook_key", Some("X-Delivery")));
    assert!(verified.check().is_err());
    let missing = with(TriggerKind::DataUploaded, Trigger::default());
    assert_eq!(
        missing,
        Err(ManifestError::MissingPermission(
            TriggerKind::DataUploaded,
            Permission::ObjectsRead
        ))
    );
}

#[test]
fn checks_names() {
    for name in ["Announcer", "-a", "a-", "1a", "a_b", ""] {
        assert!(!valid_plugin(name), "{name}");
    }
    assert!(valid_plugin("dataset-announcer"));
    for name in ["catalogUrl", "_a", "a-b", "1a", ""] {
        assert!(!valid_name(name), "{name}");
    }
    for version in ["0.1.0", "1.20.3-rc.1", "2.0.0+build.5"] {
        assert!(valid_version(version), "{version}");
    }
    for version in ["1.0", "1.0.0.0", "v1.0.0", "1..0", "1.0.0-", "1.0.0-ü"] {
        assert!(!valid_version(version), "{version}");
    }
}

#[test]
fn checks_settings() {
    let setting = |kind, default: Option<Value>, choices: &[&str]| Setting {
        title: "Value".into(),
        kind,
        default,
        choices: choices.iter().map(|c| c.to_string()).collect(),
    };
    let ok = [
        setting(SettingKind::Integer, Some(3.into()), &[]),
        setting(SettingKind::Url, Some("https://x.example".into()), &[]),
        setting(SettingKind::Choice, Some("a".into()), &["a", "b"]),
    ];
    for item in ok {
        assert_eq!(check_setting(&item), Ok(()));
    }
    let bad = [
        setting(SettingKind::Integer, Some("3".into()), &[]),
        setting(SettingKind::Url, Some("http://x.example".into()), &[]),
        setting(SettingKind::Choice, Some("c".into()), &["a", "b"]),
        setting(SettingKind::Choice, None, &[]),
        setting(SettingKind::String, None, &["a"]),
    ];
    for item in bad {
        assert!(check_setting(&item).is_err(), "{item:?}");
    }
}

#[test]
fn import_rule() {
    let manifest = example();
    let import = |name: &str| format!("aruna:plugin/{name}@0.2.0");
    let allowed = [
        "types",
        "log",
        "metadata-read",
        "metadata-write",
        "connections",
    ]
    .map(import);
    assert_eq!(
        manifest.check_imports(allowed.iter().map(String::as_str)),
        Ok(())
    );
    let objects = import("objects-read");
    assert_eq!(
        manifest.check_imports([objects.as_str()]),
        Err(ManifestError::UndeclaredImport(objects.clone()))
    );
    for name in [
        "wasi:cli/environment@0.2.0",
        "aruna:plugin/log@0.1.0",
        "aruna:plugin/files@0.2.0",
    ] {
        assert_eq!(
            manifest.check_imports([name]),
            Err(ManifestError::UnknownImport(name.into()))
        );
    }
    let mut offline = example();
    offline.connections.clear();
    let connections = import("connections");
    assert_eq!(
        offline.check_imports([connections.as_str()]),
        Err(ManifestError::UndeclaredImport(connections.clone()))
    );
}

#[test]
fn parse_bounds() {
    assert_eq!(
        Manifest::parse(&vec![b' '; MAX_MANIFEST + 1]),
        Err(ManifestError::TooLarge)
    );
    let unbuilt = serde_json::to_vec(&example()).unwrap();
    assert_eq!(
        Manifest::parse(&unbuilt),
        Err(ManifestError::UnsupportedAbi(String::new()))
    );
    assert!(matches!(
        Manifest::parse(b"{"),
        Err(ManifestError::Malformed(_))
    ));
}
