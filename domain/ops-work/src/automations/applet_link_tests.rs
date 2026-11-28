//! Tests wildcard applet triggers, outside-input approval, diffs and feature gates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::{
    Access, Applet, Embedding, Grants, Lifecycle as AppletLifecycle, StartGrant, SubmissionReceipt,
    applet_key, group_key, submission_key,
};
use aruna_core::automation::record::Trigger;
use aruna_core::automation::rules::{Condition, ConditionMode, Conditions};
use aruna_core::automation::schedule::Weekday;
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

use super::{AppletLinkTarget, LinkAppletOperation};
use crate::applets::AppletError;
use crate::automations::AutoError;
use crate::automations::change::AutoChange;
use crate::automations::diff::DiffAutoOperation;
use crate::automations::flow_tests::Env;
use crate::automations::lifecycle::ChangeAutoOperation;
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
    let target = AppletLinkTarget {
        applet_id,
        expected_revision: Some(1),
    };

    let mut operation = LinkAppletOperation::new(&auth, target, vec![auto_id], node);
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
    let mut enabled_op = LinkAppletOperation::new(&auth, target, vec![auto_id], node)
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
        identity_synced_revision: 0,
        workflow_id: None,
    };
    let cross_group = enabled_op.next(IoResult::Values(vec![Some(
        other_group.to_bytes().unwrap().into(),
    )]));
    assert!(matches!(cross_group, Err(AutoError::Invalid(_))));
}

#[tokio::test]
async fn guards_wildcard_applets() {
    let env = Env::new().await;
    let public_id = Ulid::from_bytes([51; 16]);
    let private_id = Ulid::from_bytes([52; 16]);
    let removed_id = Ulid::from_bytes([53; 16]);
    let foreign_id = Ulid::from_bytes([54; 16]);

    let mut foreign = sample_applet(&env, foreign_id, "secret-other-group", Access::Public);
    foreign.group_id = Ulid::from_bytes([99; 16]);

    for applet in [
        sample_applet(&env, public_id, "public-intake", Access::Public),
        sample_applet(&env, private_id, "private-intake", Access::Private),
        {
            let mut down = sample_applet(&env, removed_id, "old-intake", Access::Public);
            down.lifecycle = AppletLifecycle::TakenDown;
            down
        },
        foreign,
    ] {
        env.write_row(
            APPLETS_KEYSPACE,
            applet_key(applet.applet_id),
            applet.to_bytes().unwrap(),
        )
        .await;
        env.write_row(
            APPLETS_KEYSPACE,
            group_key(applet.group_id, applet.applet_id),
            Vec::new(),
        )
        .await;
    }

    // Approved wildcard automation WITHOUT allow_outside_input rejects linking a public applet.
    let mut spec = env.base_spec();
    spec.triggers = vec![Trigger::AppletSubmission { applet_id: None }];
    spec.conditions = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::Weekdays {
            days: Weekday::ALL.to_vec(),
            zone: "UTC".into(),
        }],
    };
    let automation = env.create_active(spec.clone()).await;

    let blocked = drive(
        LinkAppletOperation::new(
            &env.member,
            AppletLinkTarget {
                applet_id: public_id,
                expected_revision: Some(1),
            },
            vec![automation.automation_id],
            env.node,
        ),
        &env.ctx,
    )
    .await;
    assert!(
        matches!(blocked, Err(AutoError::Conflict(_))),
        "expected Conflict linking public applet without allow_outside_input, got {blocked:?}"
    );

    // Linking a private applet succeeds, but if it later becomes public, admission skips it.
    drive(
        LinkAppletOperation::new(
            &env.member,
            AppletLinkTarget {
                applet_id: private_id,
                expected_revision: Some(1),
            },
            vec![automation.automation_id],
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    let mut turned_public = sample_applet(&env, private_id, "private-intake", Access::Public);
    turned_public.automations = vec![automation.automation_id];
    env.write_row(
        APPLETS_KEYSPACE,
        applet_key(private_id),
        turned_public.to_bytes().unwrap(),
    )
    .await;
    let path = group_permission_path(env.realm, env.group, env.node);
    let flipped_version = Ulid::from_bytes([80; 16]);
    let flipped_receipt = SubmissionReceipt {
        applet_id: private_id,
        version_id: flipped_version,
        pseudonym: "vis-80".into(),
        guest: false,
        outside_input: true,
        bucket: "uploads".into(),
        key: "in/80.json".into(),
        files: Vec::new(),
        created_at_ms: 19,
    };
    env.write_row(
        APPLETS_KEYSPACE,
        submission_key(private_id, flipped_version),
        flipped_receipt.to_bytes().unwrap(),
    )
    .await;
    env.journal(&SourceEvent::applet_submission(
        env.group,
        path.clone(),
        (private_id, flipped_version),
        None,
        19,
    ))
    .await;
    let blocked_run = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((blocked_run.fired, blocked_run.skipped), (0, 1));

    // Approving `allow_outside_input = true` requires admin approval (widening), then allows link+fire.
    let mut outside_spec = spec;
    outside_spec.scope.allow_outside_input = true;
    let pending = drive(
        ChangeAutoOperation::new(
            &env.member,
            (automation.automation_id, Some(automation.revision)),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(outside_spec),
                workflow_id: None,
            },
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(pending.pending.is_some());
    assert!(!pending.spec.scope.allow_outside_input);
    drive(
        ChangeAutoOperation::new(
            &env.owner,
            (automation.automation_id, Some(pending.revision)),
            AutoChange::Approve,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();

    let updated = drive(
        LinkAppletOperation::new(
            &env.owner,
            AppletLinkTarget {
                applet_id: public_id,
                expected_revision: Some(1),
            },
            vec![automation.automation_id, automation.automation_id],
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(updated.automations, vec![automation.automation_id]);

    // Approval diff lists linked live applets and marks outside_input.
    let diff = drive(
        DiffAutoOperation::new(&env.owner, automation.automation_id, env.node, None),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(diff.diff.outside_input);
    assert!(
        diff.diff
            .linked_applets
            .iter()
            .any(|item| item.applet_id == public_id)
    );

    // Submissions from linked public applet now fire cleanly.
    let version_id = Ulid::from_bytes([81; 16]);
    let receipt = SubmissionReceipt {
        applet_id: public_id,
        version_id,
        pseudonym: "vis-81".into(),
        guest: false,
        outside_input: true,
        bucket: "uploads".into(),
        key: "in/81.json".into(),
        files: Vec::new(),
        created_at_ms: 20,
    };
    env.write_row(
        APPLETS_KEYSPACE,
        submission_key(public_id, version_id),
        receipt.to_bytes().unwrap(),
    )
    .await;
    env.journal(&SourceEvent::applet_submission(
        env.group,
        path,
        (public_id, version_id),
        None,
        20,
    ))
    .await;
    let report = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((report.scanned, report.fired, report.kept), (1, 1, 0));

    // Diff hides foreign-group applet metadata and merges run_finished + applet_submission flags.
    let mut start_applet = sample_applet(
        &env,
        Ulid::from_bytes([55; 16]),
        "dup-start",
        Access::Private,
    );
    start_applet.grants.start = Some(StartGrant::Workflow {
        workflow_id: Ulid::from_bytes([60; 16]),
        version: 1,
    });
    env.write_row(
        APPLETS_KEYSPACE,
        applet_key(start_applet.applet_id),
        start_applet.to_bytes().unwrap(),
    )
    .await;
    let mut mixed_spec = env.base_spec();
    mixed_spec.triggers = vec![
        Trigger::RunFinished {
            workflow_id: Ulid::from_bytes([60; 16]),
            status: None,
            applet_id: Some(start_applet.applet_id),
        },
        Trigger::AppletSubmission {
            applet_id: Some(start_applet.applet_id),
        },
        Trigger::AppletSubmission {
            applet_id: Some(foreign_id),
        },
    ];
    let mixed_auto = env.create_active(mixed_spec).await;
    let mixed_diff = drive(
        DiffAutoOperation::new(&env.owner, mixed_auto.automation_id, env.node, None),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(!mixed_diff.diff.outside_input);
    assert_eq!(mixed_diff.diff.linked_applets.len(), 1);
    assert_eq!(
        mixed_diff.diff.linked_applets[0].applet_id,
        start_applet.applet_id
    );
    assert_eq!(mixed_diff.diff.applet_warnings.len(), 1);
}
