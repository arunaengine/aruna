//! Tests datasets: fields read from the entity, a save of only what changed at the revision read,
//! a conflict, file entities, listings, creates named by their path, and the dataset subject.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::bindings::aruna::plugin::types::{Invocation, Trigger};
use crate::host::fake::{Fake, install};
use crate::{WatchEvent, handle, objects};

fn seeded() -> Fake {
    let fake = install(&[("raw/reads.fq", b"ACGT", "v1")]);
    let entity = json!({ "@id": "./", "@type": "Dataset", "name": "Run 7", "keywords": "pcr, gel",
        "license": { "@id": "CC-BY-4.0" }, "identifier": { "doi": "10.1/x" }, "publisher": "Lab" });
    fake.dataset("D1", entity);
    fake
}

#[test]
fn fields_and_save() {
    let fake = seeded();
    let mut dataset = get("D1").unwrap();
    assert_eq!(
        (dataset.name.as_str(), dataset.revision.as_str()),
        ("Run 7", "1")
    );
    assert_eq!(dataset.keywords, ["pcr", "gel"]);
    assert_eq!(dataset.license.as_deref(), Some("CC-BY-4.0"));
    assert_eq!(dataset.identifiers["doi"], "10.1/x");
    assert_eq!(dataset.get("publisher"), Some(json!("Lab")));
    assert_eq!(
        dataset.save().unwrap(),
        Vec::<String>::new(),
        "nothing changed, no call"
    );
    dataset.identifiers.insert("catalog".into(), "c-1".into());
    dataset.remove("publisher");
    dataset.description = "gels".into();
    let mut changed = dataset.save().unwrap();
    changed.sort();
    assert_eq!(changed, ["description", "identifiers", "publisher"]);
    let patch =
        r#"{"description":"gels","identifier":{"catalog":"c-1","doi":"10.1/x"},"publisher":null}"#;
    assert_eq!(fake.calls().last().unwrap(), &format!("patch D1 {patch} 1"));
    assert_eq!(dataset.revision, "2");
    // A change made meanwhile: the stale copy conflicts.
    let mut stale = get("D1").unwrap();
    dataset.name = "Run 8".into();
    dataset.save().unwrap();
    stale.name = "Run 9".into();
    assert!(matches!(stale.save(), Err(Error::Conflict(_))));
    stale.reload().unwrap();
    assert_eq!(stale.name, "Run 8");
}

#[test]
fn file_entities() {
    let fake = seeded();
    let mut dataset = get("D1").unwrap();
    let object = objects::get("raw/reads.fq").unwrap();
    dataset.add_file(&object, Some("reads/x")).unwrap();
    dataset.add_file(&object, None).unwrap();
    let files: Vec<Value> = dataset.files().map(Result::unwrap).collect();
    let expected = json!({ "@id": "reads/x", "@type": "File", "name": "reads.fq",
        "contentSize": 4, "contentUrl": "s3://raw/reads.fq" });
    assert_eq!(files[0], expected);
    assert_eq!(files[1]["@id"], "s3://raw/reads.fq");
    dataset.remove_file("reads/x").unwrap();
    assert_eq!(dataset.revision, "4");
    assert_eq!(dataset.files().count(), 1);
    assert_eq!(
        dataset.ro_crate().unwrap()["hasPart"],
        json!([{ "@id": "s3://raw/reads.fq" }])
    );
    assert!(fake.calls().iter().any(|call| call == "get D1 true"));
}

#[test]
fn list_and_create() {
    let fake = seeded();
    for n in 2..5 {
        fake.dataset(&format!("D{n}"), json!({ "name": format!("Run {n}") }));
    }
    let names: Vec<String> = list().map(|dataset| dataset.unwrap().name).collect();
    assert_eq!(names, ["Run 7", "Run 2", "Run 3", "Run 4"]);
    let fields = json!({ "name": "Zenodo 1", "identifiers": { "zenodo": "1" } });
    let created = create("zenodo/1", fields.clone()).unwrap();
    assert_eq!(created.identifiers["zenodo"], "1");
    assert_eq!(create("zenodo/1", fields).unwrap().id, created.id);
    assert!(matches!(
        create("x", json!([1])),
        Err(Error::InvalidCall(_))
    ));
}

#[test]
fn dataset_subject() {
    seeded();
    let event = WatchEvent {
        kind: "dataset_created".into(),
        path: "/r/g/x/meta/D1".into(),
        resource: "D1".into(),
        revision: "1".into(),
    };
    let invocation = |trigger| Invocation {
        run: "run-1".into(),
        install: "i".into(),
        group: "g".into(),
        settings: "{}".into(),
        trigger,
    };
    let name = |dataset: Dataset| Ok(dataset.name);
    assert_eq!(
        handle(invocation(Trigger::Event(event)), name),
        Ok(Some(r#""Run 7""#.into()))
    );
    let form = |form: crate::Form| Ok(form.dataset("dataset")?.license);
    let manual = invocation(Trigger::Manual(r#"{"dataset": "D1"}"#.into()));
    assert_eq!(handle(manual, form), Ok(Some(r#""CC-BY-4.0""#.into())));
}
