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
    // Write includes copies: read on the source and write on the target.
    let write = [
        Permission::MetadataWrite,
        Permission::ObjectsCopy,
        Permission::TransfersDownload,
    ];
    for permission in write.into_iter().chain([Permission::SecretsValue]) {
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
    assert!(required.contains(&Permission::TransfersUpload));
    assert!(!required.contains(&Permission::MetadataWrite));
    assert!(!required.contains(&Permission::ObjectsCopy));
    assert!(!required.contains(&Permission::TransfersDownload));
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
        (
            "on = [\"manual\"]\n[services.catalog]\nurl = \"https://x.example\"\n\
             [services.catalog.secret]\ntitle = \"Key\"\nmethods = [\"GET\"]\n",
            "a secret title, or a table with `title` and `kind`",
        ),
        (
            "on = [\"manual\"]\n[build]\noutput = \"x\"\n",
            "unknown field `build`",
        ),
    ];
    for (rest, expected) in cases {
        let message = error(rest);
        assert!(message.contains(expected), "{rest}: {message}");
    }
}

#[test]
fn settings_refuse_datasets() {
    let picked = "name = \"p\"\nversion = \"0.1.0\"\non = [\"manual\"]\n[settings]\nd = { type = \"dataset\" }\n";
    assert!(matches!(
        parse(picked).unwrap().check(),
        Err(ManifestError::InvalidSetting(name, _)) if name == "d"
    ));
}

const IMAGE: &str =
    "harbor.example/map@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn tasked(task: &str) -> Result<Manifest, ManifestError> {
    let head = "name = \"p\"\nversion = \"0.1.0\"\non = [\"manual\"]\naccess = \"write\"\n";
    parse(&format!("{head}[tasks.map]\nimage = \"{IMAGE}\"\n{task}"))
}

#[test]
fn task_defaults() {
    let manifest = tasked("command = [\"/opt/map.sh\", \"--ref={reference}\"]\n").unwrap();
    assert_eq!(manifest.check(), Ok(()));
    let map = &manifest.recipes["map"];
    assert_eq!((map.cpu, map.memory, map.time), (1, 2 << 30, 3600));
    assert_eq!(map.network, Network::None);
    assert_eq!(map.parameters["reference"], DEFAULT_PATTERN);
    assert!(manifest.permissions.required.contains(&Permission::JobsRun));
    let sized = tasked("cpu = 8\nmemory = \"512 MiB\"\ntime = \"2d\"\nnetwork = \"open\"\n");
    let sized = &sized.unwrap().recipes["map"];
    assert_eq!(
        (sized.cpu, sized.memory, sized.time),
        (8, 512 << 20, 172_800)
    );
    assert_eq!(sized.network, Network::Open);
}

#[test]
fn task_errors() {
    let error = |task: &str| match tasked(task) {
        Err(ManifestError::Malformed(message)) => message,
        other => panic!("{task}: {other:?}"),
    };
    let cases = [
        ("memory = \"2 GB\"\n", "memory is a whole number"),
        ("time = \"90s\"\n", "time is a whole number"),
        ("time = \"1µ\"\n", "time is a whole number"),
        (
            "parameters = { threads = \"[0-9]+\" }\n",
            "`threads` is no `{name}`",
        ),
        ("command = [\"{Threads}\"]\n", "is not a parameter `{name}`"),
        (
            "secrets = { KEY = \"Key\" }\n",
            "secrets in the container need",
        ),
    ];
    for (task, expected) in cases {
        let message = error(task);
        assert!(message.contains(expected), "{task}: {message}");
    }
    let reader = "name = \"p\"\nversion = \"0.1.0\"\non = [\"manual\"]\n";
    let read_only = parse(&format!("{reader}[tasks.map]\nimage = \"{IMAGE}\"\n"));
    assert!(matches!(read_only, Err(ManifestError::Malformed(m)) if m.contains("access")));
}

#[test]
fn task_checks() {
    let invalid = |task: &str| match tasked(task).unwrap().check() {
        Err(ManifestError::InvalidTask(name, why)) if name == "map" => why,
        other => panic!("{task}: {other:?}"),
    };
    assert!(invalid("report = \"out/report.json\"\n").contains("report"));
    assert!(invalid("report = \"/out/../report.json\"\n").contains("report"));
    assert!(invalid("command = [\"{n}\"]\nparameters = { n = \"(\" }\n").contains("pattern"));
    assert!(invalid("cpu = 0\n").contains("above zero"));
    let mut unpinned = tasked("").unwrap();
    let map = unpinned.recipes.get_mut("map").unwrap();
    map.image = "harbor.example/map:latest".into();
    assert!(
        matches!(unpinned.check(), Err(ManifestError::InvalidTask(_, why)) if why.contains("pinned"))
    );
    let mut ungranted = tasked("").unwrap();
    ungranted.permissions.required.remove(&Permission::JobsRun);
    assert!(
        matches!(ungranted.check(), Err(ManifestError::InvalidTask(_, why)) if why.contains("jobs.run"))
    );
}

#[test]
fn fills_command() {
    let manifest = tasked("command = [\"/opt/map.sh\", \"--ref={reference}\", \"{reference}\"]\n");
    let map = &manifest.unwrap().recipes["map"];
    let values = |pairs: &[(&str, &str)]| {
        (pairs.iter())
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        map.fill(&values(&[("reference", "GCF_000005845.2")])),
        Ok(vec![
            "/opt/map.sh".to_string(),
            "--ref=GCF_000005845.2".to_string(),
            "GCF_000005845.2".to_string()
        ])
    );
    // A value is one argument and may never start an option.
    assert!(map.fill(&values(&[("reference", "-rf")])).is_err());
    assert!(map.fill(&values(&[("reference", "a b")])).is_err());
    assert!(map.fill(&values(&[])).is_err());
    let extra = values(&[("reference", "a"), ("threads", "4")]);
    assert!(map.fill(&extra).is_err());
}
