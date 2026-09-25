//! Checks the shipped mapping rules and the mapping preview.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const KINDS: [RepositoryConnectorKind; 2] = [
    RepositoryConnectorKind::Invenio,
    RepositoryConnectorKind::OaiPmh,
];

#[test]
fn shipped_rules_consistent() {
    for kind in KINDS {
        let Some(rules) = rules(kind).expect("embedded rules parse") else {
            continue;
        };
        let mut names = rules
            .targets
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            rules.targets.len(),
            "{kind:?} target names repeat"
        );
        for target in &rules.targets {
            for relation in &target.relations {
                assert!(
                    rules.target(&relation.target).is_some(),
                    "{}",
                    relation.target
                );
            }
            if let Some(group) = &target.group {
                assert!(rules.target(&group.each).is_some(), "{}", group.each);
            }
            let mut fields = target
                .fields
                .iter()
                .map(|f| f.field.as_str())
                .collect::<Vec<_>>();
            fields.sort_unstable();
            fields.dedup();
            assert_eq!(
                fields.len(),
                target.fields.len(),
                "{} fields repeat",
                target.name
            );
            assert!(target.fields.iter().all(|field| !field.property.is_empty()));
        }
    }
    let invenio = rules(RepositoryConnectorKind::Invenio).unwrap().unwrap();
    assert!(invenio.target("record").is_some_and(|t| t.select.root));
    assert_eq!(invenio.target("file").unwrap().content.max_files, Some(100));
}

#[test]
fn unknown_rules_refused() {
    let unknown = "[[targets]]\nname = \"record\"\nselect = { root = true }\n\
        fields = [{ property = [\"name\"], field = \"title\", convert = \"shout\" }]";
    assert!(toml::from_str::<Rules>(unknown).is_err());
    assert!(
        toml::from_str::<Rules>("[[targets]]\nname = \"x\"\nselect = { roots = true }").is_err()
    );
}
