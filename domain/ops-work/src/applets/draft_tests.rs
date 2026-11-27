//! Tests how build outputs become a draft manifest and how drafts move through the record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::applets::change::apply;
use crate::applets::change::tests::{applet, version};

fn draft(seed: u8) -> Draft {
    Draft {
        build_id: Ulid::from_bytes([seed; 16]),
        job_id: Ulid::from_bytes([seed; 16]),
        commit: "abc".into(),
        started_by: applet().created_by,
        started_at_ms: 1,
        expires_at_ms: 100,
        manifest: None,
        failed: None,
    }
}

fn output(applet: &Applet, draft: &Draft, path: &str, digest: &str) -> OutputObject {
    OutputObject {
        node_id: applet.host_node,
        bucket: held_bucket(applet.group_id, applet.host_node),
        key: format!(
            "{}/out/{path}",
            build_prefix(applet.applet_id, draft.build_id)
        ),
        version_id: Ulid::from_bytes([5; 16]),
        execution_id: Ulid::from_bytes([6; 16]),
        container_path: format!("/out/{path}"),
        size: 3,
        digest: Some(digest.to_string()),
    }
}

// The source map is pinned apart and the lockfile only names packages; neither is served.
#[test]
fn manifest_hides_extras() {
    let (applet, draft) = (applet(), draft(1));
    let lockfile = br#"{"packages":{"node_modules/vue":{"version":"3.5.42"}}}"#;
    let digest = blake3::hash(lockfile).to_hex().to_string();
    let outputs = vec![
        output(&applet, &draft, "index.html", "aa"),
        output(&applet, &draft, "assets/app.js", "bb"),
        output(&applet, &draft, SOURCE_MAP, "cc"),
        output(&applet, &draft, LOCKFILE, &digest),
    ];
    let manifest = draft_manifest(&applet, &draft, &outputs, Some(lockfile)).unwrap();
    let paths: Vec<&str> = manifest
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect();
    assert_eq!(paths, vec!["assets/app.js", "index.html"]);
    assert_eq!(manifest.source_map.unwrap().blake3, "cc");
    assert_eq!(manifest.packages[0].name, "vue");
    assert_eq!(manifest.commit.as_deref(), Some("abc"));

    let changed = draft_manifest(&applet, &draft, &outputs, Some(b"{}"));
    assert!(matches!(changed, Err(AppletError::Invalid(_))));
    let mut foreign = outputs.clone();
    foreign[0].bucket = "other".into();
    let refused = draft_manifest(&applet, &draft, &foreign, Some(lockfile));
    assert!(matches!(refused, Err(AppletError::Invalid(_))));
    let mut unknown = outputs;
    unknown[0].digest = None;
    let refused = draft_manifest(&applet, &draft, &unknown, Some(lockfile));
    assert!(matches!(refused, Err(AppletError::Invalid(_))));
}

// A draft is captured once, kept as the next private version, and leaves room behind it.
#[test]
fn drafts_become_versions() {
    let mut applet = applet();
    let actor = applet.created_by;
    let start = |seed| AppletChange::StartBuild {
        draft: Box::new(draft(seed)),
        max: 1,
        ttl_ms: 10,
    };
    apply(&mut applet, start(1), actor, 1).unwrap();
    assert_eq!(applet.drafts[0].expires_at_ms, 11);
    let full = apply(&mut applet, start(2), actor, 1);
    assert!(matches!(full, Err(AppletError::Conflict(_))));

    let build_id = Ulid::from_bytes([1; 16]);
    let manifest = version(1).manifest;
    let keep = AppletChange::KeepDraft {
        build_id,
        entry: Box::new(version(1)),
    };
    let early = apply(&mut applet, keep.clone(), actor, 2);
    assert!(matches!(early, Err(AppletError::Conflict(_))));
    let capture = AppletChange::CaptureDraft { build_id, manifest };
    apply(&mut applet, capture.clone(), actor, 2).unwrap();
    assert!(matches!(
        apply(&mut applet, capture, actor, 2),
        Err(AppletError::Conflict(_))
    ));
    apply(&mut applet, keep, actor, 3).unwrap();
    assert!(applet.drafts.is_empty());
    assert_eq!(applet.versions.len(), 1);

    apply(&mut applet, start(3), actor, 4).unwrap();
    let drop = AppletChange::DropDraft {
        build_id: Ulid::from_bytes([3; 16]),
    };
    apply(&mut applet, drop.clone(), actor, 5).unwrap();
    assert!(matches!(
        apply(&mut applet, drop, actor, 5),
        Err(AppletError::NotFound)
    ));
}
