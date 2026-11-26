//! Tests copy stage bucket auth, quota ceiling, distribution hints, reference size and resolve.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use aruna_core::automation::firing::{CopiedFile, Firing, FiringState, StageState, TriggerPayload};
use aruna_core::automation::keys::copy_plan_key;
use aruna_core::automation::record::{Spec, Stage, StageKind, Trigger, When};
use aruna_core::automation::scope::Scope;
use aruna_core::bucket_read::GetBucketError;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::keyspaces::{AUTOMATION_INTENTS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::workflow_run::RunFile;
use aruna_core::structs::identity::realm::{GroupQuotaOverride, RealmConfigDocument};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::distribution::hint::SetHint;
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionRevision};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use aruna_ops_data::s3::object::head::{HeadObjectError, HeadObjectResult};
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::document_repository::GetConfigError;
use aruna_runner::driver::drive;
use ulid::Ulid;

use super::AutoError;
use super::control::ResolveStageOperation;
use super::copy_stage::{CopyStageOp, StageOutcome, call_intent, stage_intent_id};
use super::flow_tests::Env;
use super::start::new_firing_rows;
use crate::plugins::intents::{Begun, IntentRecord, IntentState, intent_key, unknown_row_in};

#[tokio::test]
async fn copy_stage_checks() {
    let env = Env::new().await;
    let wf_id = Ulid::from_bytes([62; 16]);
    let write_pat = format!(
        "{}/**",
        object_permission_path(env.realm, env.group, env.node, "out", "d")
    );
    let read_pat = format!(
        "{}/**",
        object_permission_path(env.realm, env.group, env.node, "src", "r")
    );
    let mut spec = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        stages: vec![
            Stage {
                name: "run".into(),
                needs: Some(vec![]),
                when: When::Passed,
                kind: StageKind::Run {
                    workflow_id: wf_id,
                    version: 1,
                    values_json: "{}".into(),
                    inputs_json: None,
                    output_bucket: None,
                    output_prefix: None,
                    placement_site: None,
                    secrets: BTreeMap::new(),
                    provenance_crate: false,
                },
            },
            Stage {
                name: "copy".into(),
                needs: Some(vec!["run".into()]),
                when: When::Passed,
                kind: StageKind::Copy {
                    from_stage: "run".into(),
                    file: None,
                    dest_bucket: "out".into(),
                    dest_template: "d/{filename}".into(),
                    allow_downgrade: false,
                },
            },
        ],
        scope: Scope {
            read: vec![read_pat],
            write: vec![write_pat],
            workflows: vec![(wf_id, 1)],
            ..Scope::default()
        },
    };
    spec.scope.limits.bytes_per_day = 500;
    let auto = env.create_active(spec.clone()).await;

    let v1 = Ulid::from_parts(10, 1);
    let v2 = Ulid::from_parts(10, 2);
    let mut firing = Firing::new(
        Ulid::from_bytes([103; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        10,
    );
    firing.stages[0].state = StageState::Succeeded;
    firing.stages[0].receipt.outputs = vec![
        RunFile {
            name: "f1.dat".into(),
            bucket: Some("src".into()),
            key: Some("r/f1.dat".into()),
            version_id: Some(v1.to_string()),
            node_id: Some(env.node),
            size: Some(1000),
            digest: None,
        },
        RunFile {
            name: "f2.dat".into(),
            bucket: Some("src".into()),
            key: Some("r/f2.dat".into()),
            version_id: Some(v2.to_string()),
            node_id: Some(env.node),
            size: Some(200),
            digest: None,
        },
    ];
    firing.evaluate(11);

    let bucket = |group_id, dist| BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: env.owner.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: dist,
    };
    let ref_head = |len| HeadObjectResult {
        location: None,
        metadata: HashMap::new(),
        source_metadata: Some(SourceMetadata {
            content_length: len,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: BTreeMap::new(),
        }),
        last_refresh: None,
        version_created_at: None,
        version_id: Some(v1),
        resolved_version_id: Some(v1),
        checksum_type: MultipartChecksumType::FullObject,
        composite_hashes: HashMap::new(),
        part_count: None,
        source_policies: Vec::new(),
        source_binding: None,
    };

    // Destination bucket owned by another group is rejected before copying.
    let other_group = Ulid::from_bytes([77; 16]);
    let mut op = CopyStageOp::new(auto.clone(), firing.clone(), 1, (env.node, env.realm)).unwrap();
    op.stamp(1_000);
    op.first().unwrap();
    op.next(IoResult::Child(ChildOutcome::new(Ok::<
        _,
        crate::plugins::rows::StoreError,
    >(Begun::Fresh))))
        .unwrap();
    op.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetBucketError>(
        bucket(env.group, None),
    ))))
    .unwrap();
    op.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetBucketError>(
        bucket(other_group, None),
    ))))
    .unwrap();
    let step = op
        .next(IoResult::Child(ChildOutcome::new(
            Ok::<_, HeadObjectError>(ref_head(100)),
        )))
        .unwrap();
    assert!(matches!(step, Next::Call(IoCall::Delete(_))));
    let done = op.next(IoResult::Done).unwrap();
    assert!(matches!(
        done,
        Next::Done(StageOutcome::Failed(msg)) if msg.contains("outside scope.write")
    ));

    // Reference-backed size counts toward daily bytes; distribution hint is minted on plan.
    let set_id = Ulid::from_bytes([88; 16]);
    let dist = DistributionBinding {
        set_id,
        member_id: Ulid::from_bytes([89; 16]),
        incarnation: 1,
        revision: DistributionRevision {
            number: 1,
            digest: [0; 32],
        },
    };
    let mut cfg = RealmConfigDocument::default_for_realm(env.realm, Vec::new());
    cfg.quota.group_overrides.push(GroupQuotaOverride {
        group_id: env.group,
        quota_bytes: Some(2_000),
        grace_factor_percent: Some(100),
    });
    let mut op2 = CopyStageOp::new(auto.clone(), firing.clone(), 1, (env.node, env.realm)).unwrap();
    op2.stamp(1_000);
    op2.seed(42);
    op2.first().unwrap();
    op2.next(IoResult::Child(ChildOutcome::new(Ok::<
        _,
        crate::plugins::rows::StoreError,
    >(Begun::Fresh))))
        .unwrap();
    op2.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetBucketError>(
        bucket(env.group, None),
    ))))
    .unwrap();
    op2.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetBucketError>(
        bucket(env.group, Some(dist.clone())),
    ))))
    .unwrap();
    op2.next(IoResult::Child(ChildOutcome::new(
        Ok::<_, HeadObjectError>(ref_head(1_000)),
    )))
    .unwrap();
    op2.next(IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizeError>(
        (),
    ))))
    .unwrap();
    op2.next(IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizeError>(
        (),
    ))))
    .unwrap();
    let count_call = op2
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            cfg.clone(),
        ))))
        .unwrap();
    assert!(matches!(count_call, Next::Call(IoCall::Child(_))));
    let over = op2
        .next(IoResult::Child(ChildOutcome::new(Err::<(), AutoError>(
            AutoError::Limited("cap".into()),
        ))))
        .unwrap();
    assert!(matches!(over, Next::Call(IoCall::Delete(_))));
    let failed = op2.next(IoResult::Done).unwrap();
    assert!(matches!(failed, Next::Done(StageOutcome::Failed(_))));

    let mut op3 = CopyStageOp::new(auto.clone(), firing.clone(), 1, (env.node, env.realm)).unwrap();
    op3.stamp(1_000);
    op3.seed(42);
    op3.first().unwrap();
    op3.next(IoResult::Child(ChildOutcome::new(Ok::<
        _,
        crate::plugins::rows::StoreError,
    >(Begun::Fresh))))
        .unwrap();
    op3.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetBucketError>(
        bucket(env.group, None),
    ))))
    .unwrap();
    op3.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetBucketError>(
        bucket(env.group, Some(dist.clone())),
    ))))
    .unwrap();
    op3.next(IoResult::Child(ChildOutcome::new(
        Ok::<_, HeadObjectError>(ref_head(100)),
    )))
    .unwrap();
    op3.next(IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizeError>(
        (),
    ))))
    .unwrap();
    op3.next(IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizeError>(
        (),
    ))))
    .unwrap();
    op3.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
        cfg,
    ))))
    .unwrap();
    let plan_write = op3
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, AutoError>(()))))
        .unwrap();
    let Next::Call(IoCall::Write(rows)) = plan_write else {
        panic!("expected copy plan write: {plan_write:?}");
    };
    let planned: CopiedFile = postcard::from_bytes(&rows[0].2).unwrap();
    assert_eq!(SetHint::of_version(planned.dest_version), dist.hint());

    // Resolving file 1 of 2 as done recovers planned receipt and keeps stage Running.
    let stage_id = stage_intent_id(firing.firing_id, "copy");
    let intent = call_intent(
        0,
        &[b"src", b"r/f1.dat", &v1.to_bytes(), b"out", b"d/{filename}"],
    );
    let (_, unk_key, unk_val) =
        unknown_row_in(AUTOMATION_INTENTS_KEYSPACE, stage_id, &intent, "timeout").unwrap();
    env.write_row(AUTOMATION_INTENTS_KEYSPACE, unk_key, unk_val)
        .await;
    env.write_row(
        AUTOMATION_RUNS_KEYSPACE,
        copy_plan_key(stage_id, 0),
        postcard::to_allocvec(&planned).unwrap(),
    )
    .await;
    firing.stages[1].state = StageState::NeedsAttention;
    firing.state = FiringState::NeedsAttention;
    for (ks, k, v) in new_firing_rows(&firing).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    let resolved = drive(
        ResolveStageOperation::new(
            &env.owner,
            firing.firing_id,
            "copy".into(),
            true,
            Some("verified".into()),
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(resolved.stages[1].state, StageState::Running);
    assert_eq!(resolved.state, FiringState::Running);
    assert_eq!(resolved.stages[1].receipt.copied, vec![planned]);
    assert!(
        env.read_row(AUTOMATION_RUNS_KEYSPACE, &copy_plan_key(stage_id, 0))
            .await
            .is_none()
    );
    let settled_raw = env
        .read_row(AUTOMATION_INTENTS_KEYSPACE, &intent_key(stage_id, 0))
        .await
        .unwrap();
    let settled: IntentRecord = postcard::from_bytes(&settled_raw).unwrap();
    assert!(matches!(settled.state, IntentState::Settled { .. }));
}
