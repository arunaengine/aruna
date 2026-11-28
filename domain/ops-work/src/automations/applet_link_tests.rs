//! Tests wildcard applet triggers, applet automation linking, approval diffs and feature gates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::{
    Access, Applet, Embedding, Grants, Lifecycle as AppletLifecycle, SubmissionReceipt, applet_key,
    group_key, submission_key,
};
use aruna_core::automation::record::Trigger;
use aruna_core::automation::rules::{Condition, ConditionMode, Conditions, Weekday};
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::keyspaces::APPLETS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::structs::identity::features::{Feature, FeatureMode};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_ops_identity::document_repository::GetConfigError;
use aruna_runner::driver::drive;
use ulid::Ulid;

use super::LinkAppletOperation;
use crate::applets::AppletError;
use crate::automations::AutoError;
use crate::automations::diff::DiffAutoOperation;
use crate::automations::flow_tests::Env;
use crate::automations::matcher::MatchEventsOperation;

fn sample_applet(env: &Env, applet_id: Ulid, slug: &str, access: Access) -> Applet {
    Applet {
        applet_id,
        group_id: env.group,
        name: format!("Applet {slug}"),
        slug: slug.into(),
        host: format!("{slug}.apps.local"),
        repository: None,
        host_node: env.node,
        lifecycle: AppletLifecycle::Live,
        access,
        live: Some(1),
        versions: Vec::new(),
        drafts: Vec::new(),
        grants: Grants::default(),
        embedding: Embedding::default(),
        guests: None,
        domains: Vec::new(),
        pending: None,
        account: None,
        role_id: None,
        visitor_salt: [5; 32],
        created_by: env.owner.user_id,
        created_at_ms: 1,
        revision: 1,
        automations: Vec::new(),
    }
}

#[test]
fn steps_link_phases() {
    let realm = aruna_core::structs::identity::realm::RealmId([6; 32]);
    let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
    let group_id = Ulid::from_bytes([8; 16]);
    let applet_id = Ulid::from_bytes([9; 16]);
    let auto_id = Ulid::from_bytes([10; 16]);
    let auth = aruna_core::structs::identity::auth::AuthContext::new(
        aruna_core::user::id::UserId::local(Ulid::from_bytes([1; 16]), realm),
        realm,
    );

    let mut operation = LinkAppletOperation::new(&auth, (applet_id, Some(1)), vec![auto_id], node);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));

    let applet = Applet {
        applet_id,
        group_id,
        name: "Intake".into(),
        slug: "intake".into(),
        host: "intake.apps.local".into(),
        repository: None,
        host_node: node,
        lifecycle: AppletLifecycle::Live,
        access: Access::Private,
        live: Some(1),
        versions: Vec::new(),
        drafts: Vec::new(),
        grants: Grants::default(),
        embedding: Embedding::default(),
        guests: None,
        domains: Vec::new(),
        pending: None,
        account: None,
        role_id: None,
        visitor_salt: [0; 32],
        created_by: auth.user_id,
        created_at_ms: 1,
        revision: 1,
        automations: Vec::new(),
    };
    assert!(matches!(
        operation.next(IoResult::Child(ChildOutcome::new(Ok::<_, AppletError>(
            applet.clone()
        )))),
        Ok(Next::Call(IoCall::Child(_)))
    ));

    // Disabled Automations feature refuses non-empty link inside the domain operation.
    let mut disabled_config = RealmConfigDocument::default_for_realm(realm, Vec::new());
    disabled_config.features.insert(
        Feature::Applets,
        FeatureMode::Scoped {
            default: true,
            groups: Default::default(),
        },
    );
    disabled_config.features.insert(
        Feature::Automations,
        FeatureMode::Scoped {
            default: false,
            groups: Default::default(),
        },
    );
    let refused = operation.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
        disabled_config,
    ))));
    assert!(matches!(
        refused,
        Err(AutoError::FeatureOff(Feature::Automations, _))
    ));

    // Enabled features advance to batch-reading target automations, then rejecting cross-group IDs.
    let mut enabled_op = LinkAppletOperation::new(&auth, (applet_id, Some(1)), vec![auto_id], node)
        .with_disabled_features(BTreeSet::new());
    assert!(matches!(
        enabled_op.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(matches!(
        enabled_op.next(IoResult::Child(ChildOutcome::new(Ok::<_, AppletError>(
            applet
        )))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let mut enabled_config = RealmConfigDocument::default_for_realm(realm, Vec::new());
    for feature in [Feature::Applets, Feature::Automations] {
        enabled_config.features.insert(
            feature,
            FeatureMode::Scoped {
                default: true,
                groups: Default::default(),
            },
        );
    }
    let read_call = enabled_op
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config,
        ))))
        .unwrap();
    assert!(matches!(read_call, Next::Call(IoCall::Read(ref keys)) if keys.len() == 1));

    let other_group = aruna_core::automation::record::Automation {
        automation_id: auto_id,
        group_id: Ulid::from_bytes([99; 16]),
        name: "other".into(),
        description: String::new(),
        host_node: node,
        lifecycle: aruna_core::automation::record::Lifecycle::Active,
        pause_reason: None,
        spec: aruna_core::automation::record::Spec::default(),
        pending: None,
        account: None,
        role_id: None,
        approved_by: None,
        approved_at_ms: None,
        created_by: auth.user_id,
        created_at_ms: 1,
        updated_at_ms: 1,
        revision: 1,
        workflow_id: None,
    };
    let cross_group = enabled_op.next(IoResult::Values(vec![Some(
        other_group.to_bytes().unwrap().into(),
    )]));
    assert!(matches!(cross_group, Err(AutoError::Invalid(_))));
}

#[tokio::test]
async fn matches_wildcard_applets() {
    let env = Env::new().await;
    let linked_id = Ulid::from_bytes([51; 16]);
    let unlinked_id = Ulid::from_bytes([52; 16]);
    let removed_id = Ulid::from_bytes([53; 16]);

    for applet in [
        sample_applet(&env, linked_id, "public-intake", Access::Public),
        sample_applet(&env, unlinked_id, "private-other", Access::Private),
        {
            let mut down = sample_applet(&env, removed_id, "old-intake", Access::Public);
            down.lifecycle = AppletLifecycle::TakenDown;
            down
        },
    ] {
        env.write_row(
            APPLETS_KEYSPACE,
            applet_key(applet.applet_id),
            applet.to_bytes().unwrap(),
        )
        .await;
        env.write_row(
            APPLETS_KEYSPACE,
            group_key(env.group, applet.applet_id),
            Vec::new(),
        )
        .await;
    }

    let mut spec = env.base_spec();
    spec.triggers = vec![Trigger::AppletSubmission { applet_id: None }];
    spec.conditions = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::Weekdays {
            days: Weekday::ALL.to_vec(),
            zone: "UTC".into(),
        }],
    };
    let automation = env.create_active(spec).await;

    // Link the live public applet via the domain operation.
    let updated = drive(
        LinkAppletOperation::new(
            &env.owner,
            (linked_id, Some(1)),
            vec![automation.automation_id, automation.automation_id],
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(updated.automations, vec![automation.automation_id]);

    // Approval diff lists linked live applets, marks outside_input, and reports conditions.
    let diff = drive(
        DiffAutoOperation::new(&env.owner, automation.automation_id, env.node, None),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(diff.diff.outside_input);
    assert_eq!(diff.diff.linked_applets.len(), 1);
    assert_eq!(diff.diff.linked_applets[0].applet_id, linked_id);
    assert!(diff.diff.linked_applets[0].outside_input);

    // Submissions from linked applet fire; submissions from unlinked applet are ignored.
    let path = group_permission_path(env.realm, env.group, env.node);
    for (seed, applet_id) in [(81u8, linked_id), (82u8, unlinked_id)] {
        let version_id = Ulid::from_bytes([seed; 16]);
        let receipt = SubmissionReceipt {
            applet_id,
            version_id,
            pseudonym: format!("vis-{seed}"),
            guest: false,
            outside_input: applet_id == linked_id,
            bucket: "uploads".into(),
            key: format!("in/{seed}.json"),
            files: Vec::new(),
            created_at_ms: 20,
        };
        env.write_row(
            APPLETS_KEYSPACE,
            submission_key(applet_id, version_id),
            receipt.to_bytes().unwrap(),
        )
        .await;
        let event = SourceEvent::applet_submission(
            env.group,
            path.clone(),
            (applet_id, version_id),
            None,
            20,
        );
        env.journal(&event).await;
    }

    let report = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((report.scanned, report.fired, report.kept), (2, 1, 0));
}
