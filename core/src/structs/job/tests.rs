use super::*;

fn secret(seed: u8) -> iroh::SecretKey {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[0] = seed;
    iroh::SecretKey::from_bytes(&seed_bytes)
}

fn node_id(seed: u8) -> NodeId {
    secret(seed).public()
}

fn user(realm: u8, byte: u8) -> UserId {
    UserId::new(Ulid::from_bytes([byte; 16]), RealmId([realm; 32]))
}

#[test]
fn digest_ignores_minter() {
    let document_id = Ulid::from_bytes([1; 16]);
    let first = JobPayload::MintPersistentId(MintPersistentIdSpec {
        document_id,
        minted_by: user(1, 2),
    });
    let second = JobPayload::MintPersistentId(MintPersistentIdSpec {
        document_id,
        minted_by: user(3, 4),
    });
    assert_eq!(first.plan_digest(), second.plan_digest());
}

fn probe_record(job_id: JobId, created_at_ms: u64) -> JobRecord {
    JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps: 3,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: Some("/tmp/probe-marker".to_string()),
        },
        user(1, 2),
        node_id(7),
        created_at_ms,
        created_at_ms,
        Some(b"dedup".to_vec()),
    )
}

#[test]
fn legal_transitions() {
    let legal = [
        (JobState::Queued, JobState::Claimed),
        (JobState::Queued, JobState::Cancelled),
        (JobState::Claimed, JobState::Running),
        (JobState::Claimed, JobState::Queued),
        (JobState::Claimed, JobState::Cancelled),
        (JobState::Claimed, JobState::Failed),
        (JobState::Running, JobState::Succeeded),
        (JobState::Running, JobState::Failed),
        (JobState::Running, JobState::Cancelled),
        (JobState::Running, JobState::Queued),
    ];
    for (from, to) in legal {
        assert!(
            validate_transition(JobExecutionClass::InProcess, from, to).is_ok(),
            "{from:?} -> {to:?}"
        );
    }
}

#[test]
fn illegal_transitions() {
    let illegal = [
        (JobState::Queued, JobState::Running),
        (JobState::Queued, JobState::Succeeded),
        (JobState::Queued, JobState::Failed),
        (JobState::Queued, JobState::Queued),
        (JobState::Claimed, JobState::Succeeded),
        (JobState::Claimed, JobState::Claimed),
        (JobState::Running, JobState::Claimed),
        (JobState::Running, JobState::Running),
    ];
    for (from, to) in illegal {
        assert_eq!(
            validate_transition(JobExecutionClass::InProcess, from, to),
            Err(JobTransitionError { from, to }),
            "{from:?} -> {to:?}"
        );
    }
}

#[test]
fn internal_rejects_external() {
    // External-only states must be rejected for an in-process job. Parking on
    // local exhaustion is the one Indeterminate edge both classes share.
    let external_only = [
        (JobState::Claimed, JobState::Preparing),
        (JobState::Preparing, JobState::Ready),
        (JobState::Ready, JobState::Running),
        (JobState::Running, JobState::Cancelling),
        (JobState::Cancelling, JobState::Cancelled),
        (JobState::Indeterminate, JobState::Running),
    ];
    for (from, to) in external_only {
        assert_eq!(
            validate_transition(JobExecutionClass::InProcess, from, to),
            Err(JobTransitionError { from, to }),
            "in-process must reject {from:?} -> {to:?}"
        );
    }
}

#[test]
fn external_graph_legal() {
    // The fenced execution graph is accepted for external attempts.
    let legal = [
        (JobState::Claimed, JobState::Preparing),
        (JobState::Preparing, JobState::Ready),
        (JobState::Preparing, JobState::Queued),
        (JobState::Ready, JobState::Running),
        (JobState::Ready, JobState::Queued),
        // A permanent pre-attempt failure terminalizes without a container.
        (JobState::Preparing, JobState::Failed),
        (JobState::Ready, JobState::Failed),
        // A cancel before the attempt intent is written terminalizes without a
        // container; without these a cancel cannot land until the job is Running.
        (JobState::Preparing, JobState::Cancelled),
        (JobState::Ready, JobState::Cancelled),
        // A submit with an unknowable outcome parks after the intent write.
        (JobState::Ready, JobState::Indeterminate),
        (JobState::Ready, JobState::Cancelling),
        (JobState::Running, JobState::Cancelling),
        (JobState::Running, JobState::Indeterminate),
        (JobState::Cancelling, JobState::Cancelled),
        (JobState::Cancelling, JobState::Succeeded),
        (JobState::Cancelling, JobState::Failed),
        (JobState::Cancelling, JobState::Indeterminate),
        (JobState::Indeterminate, JobState::Running),
        (JobState::Indeterminate, JobState::Succeeded),
    ];
    for (from, to) in legal {
        assert!(
            validate_transition(JobExecutionClass::ExternalAttempt, from, to).is_ok(),
            "external must accept {from:?} -> {to:?}"
        );
    }
    // Ready cannot skip straight to Succeeded.
    assert!(
        validate_transition(
            JobExecutionClass::ExternalAttempt,
            JobState::Ready,
            JobState::Succeeded,
        )
        .is_err()
    );
}

#[test]
fn terminal_absorbs() {
    for class in [
        JobExecutionClass::InProcess,
        JobExecutionClass::ExternalAttempt,
    ] {
        for from in [JobState::Succeeded, JobState::Failed, JobState::Cancelled] {
            for to in [
                JobState::Queued,
                JobState::Claimed,
                JobState::Preparing,
                JobState::Ready,
                JobState::Running,
                JobState::Cancelling,
                JobState::Indeterminate,
                JobState::Succeeded,
                JobState::Failed,
                JobState::Cancelled,
            ] {
                assert_eq!(
                    validate_transition(class, from, to),
                    Err(JobTransitionError { from, to }),
                    "terminal {from:?} must reject -> {to:?}"
                );
            }
        }
    }
}

fn input(key: &str, mode: InputMode, version: Option<&str>) -> InputSelection {
    InputSelection {
        source: InputSource::S3 {
            bucket: "src".to_string(),
            key: key.to_string(),
            version_id: version.map(str::to_string),
        },
        source_node_id: None,
        dest_key: format!("in/{key}"),
        mode,
        container_path: Some(format!("/inputs/{key}")),
        name: None,
        description: None,
    }
}

fn colliding() -> Vec<InputSelection> {
    let mut second = input("a", InputMode::Snapshot, None);
    second.source = InputSource::S3 {
        bucket: "other".to_string(),
        key: "a".to_string(),
        version_id: None,
    };
    vec![input("a", InputMode::Snapshot, None), second]
}

#[test]
fn rejects_key_conflict() {
    assert_eq!(
        plan_composition(colliding(), CollisionPolicy::Reject),
        Err(CompositionError::KeyConflict("in/a".to_string()))
    );
}

#[test]
fn resolves_key_conflict() {
    // Replace keeps the last claim on a key, KeepExisting the first.
    let replaced = plan_composition(colliding(), CollisionPolicy::Replace).unwrap();
    assert_eq!(replaced.len(), 1);
    assert_eq!(replaced[0].source, colliding()[1].source);

    let kept = plan_composition(colliding(), CollisionPolicy::KeepExisting).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].source, colliding()[0].source);
}

#[test]
fn enforces_reference_versions() {
    assert_eq!(
        plan_composition(
            vec![input("a", InputMode::ExactReference, None)],
            CollisionPolicy::Reject
        ),
        Err(CompositionError::MissingVersion("in/a".to_string()))
    );
    assert_eq!(
        plan_composition(
            vec![input("a", InputMode::FloatingReference, Some("01ARZ"))],
            CollisionPolicy::Reject
        ),
        Err(CompositionError::PinnedVersion("in/a".to_string()))
    );
    assert!(
        plan_composition(
            vec![
                input("a", InputMode::ExactReference, Some("01ARZ")),
                input("b", InputMode::FloatingReference, None),
            ],
            CollisionPolicy::Reject
        )
        .is_ok()
    );
}

#[test]
fn resolves_workspace_outputs() {
    // Intents materialize against the derived bucket and drain once.
    let node_id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
    let mut spec = ExecutionSpec {
        group_id: Ulid::from_bytes([2u8; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: vec![WorkspaceOutput {
            container_path: "/out/report.txt".to_string(),
            dest_key: "outputs/report.txt".to_string(),
        }],
        output_prefixes: vec!["outputs/".to_string()],
        collision_policy: Default::default(),
    };

    spec.resolve_outputs("ws-job", node_id);

    assert!(spec.workspace_outputs.is_empty());
    assert_eq!(
        spec.file_outputs,
        vec![OutputSelection {
            container_path: "/out/report.txt".to_string(),
            path_prefix: None,
            destination_node_id: Some(node_id),
            destination: OutputDestination::S3 {
                bucket: "ws-job".to_string(),
                key: "outputs/report.txt".to_string(),
            },
            name: None,
            description: None,
        }]
    );

    spec.resolve_outputs("ws-job", node_id);
    assert_eq!(spec.file_outputs.len(), 1);
}

#[test]
fn external_name_deterministic() {
    let id = JobId::from_bytes([0xAB; 16]);
    let name = attempt_external_name(id, 2);
    assert!(name.starts_with("aruna-"));
    assert!(name.ends_with("-a2"));
    assert_eq!(name, name.to_lowercase());
    assert_eq!(name, attempt_external_name(id, 2));
}

#[test]
fn dedup_key_namespaces() {
    let job = JobId::from_bytes([1u8; 16]);
    let user_a = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let user_b = UserId::new(Ulid::from_bytes([3u8; 16]), RealmId([1u8; 32]));

    assert!(crate_dedup_key(job).starts_with(b"internal/"));
    assert!(cleanup_dedup_key(job).starts_with(b"internal/"));
    assert_ne!(cleanup_dedup_key(job), crate_dedup_key(job));
    assert!(user_dedup_key(user_a, "k").starts_with(b"user/"));
    // A caller cannot forge an internal obligation key through their
    // idempotency key, and users cannot squat each other's keys.
    assert_ne!(
        user_dedup_key(user_a, &format!("internal/run-crate/{job}")),
        crate_dedup_key(job)
    );
    assert_ne!(user_dedup_key(user_a, "k"), user_dedup_key(user_b, "k"));
    assert_eq!(user_dedup_key(user_a, "k"), user_dedup_key(user_a, "k"));
    assert_eq!(cleanup_job_id(job), cleanup_job_id(job));
    assert_ne!(cleanup_job_id(job), crate_job_id(job));
    assert_eq!(workspace_credential_id(job), format!("ws{job}"));
}

#[test]
fn routable_child_ids() {
    // Child obligations remain on a non-default, high parent bucket.
    let parent = JobId::from_parts(
        1,
        PlacementHandle::new(crate::structs::FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(3_000).unwrap(),
        4,
    )
    .unwrap();
    assert_eq!(crate_job_id(parent), crate_job_id(parent));
    assert_eq!(cleanup_job_id(parent), cleanup_job_id(parent));
    assert_ne!(crate_job_id(parent), cleanup_job_id(parent));
    for child in [crate_job_id(parent), cleanup_job_id(parent)] {
        let routed = child.as_routable().unwrap();
        assert_eq!(
            routed.placement_handle(),
            parent.as_routable().unwrap().placement_handle()
        );
        assert_eq!(routed.bucket(), parent.as_routable().unwrap().bucket());
    }
}

#[test]
fn rejects_plain_ids() {
    let plain = Ulid::from_parts(1, 0);
    assert!(JobId::try_from_bytes(plain.to_bytes()).is_err());
    assert!(plain.to_string().parse::<JobId>().is_err());
    let encoded = postcard::to_allocvec(&plain).unwrap();
    assert!(postcard::from_bytes::<JobId>(&encoded).is_err());
}

#[test]
fn dedup_value_roundtrips() {
    let id = JobId::from_bytes([7u8; 16]);
    let digest = [9u8; 32];
    let encoded = encode_dedup_value(id, digest);
    assert_eq!(encoded.len(), 48);
    assert_eq!(parse_dedup_value(&encoded).unwrap(), (id, digest));
    assert!(parse_dedup_value(&encoded[..16]).is_err());
}

#[test]
fn record_roundtrips() {
    let record = probe_record(JobId::from_bytes([5u8; 16]), 1_700_000_000_000);
    let bytes = record.to_bytes().unwrap();
    assert_eq!(JobRecord::from_bytes(&bytes).unwrap(), record);
}

#[test]
fn rocrate_record_roundtrips() {
    let owner = user(1, 2);
    let record = JobRecord::new(
        JobId::from_bytes([5u8; 16]),
        JobPayload::ImportRoCrate(ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: owner,
                realm_id: RealmId([1u8; 32]),
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload {
                upload_id: Ulid::from_bytes([3u8; 16]),
            },
            target: ImportRoCrateTarget {
                bucket: "target".to_string(),
                prefix: "crate".to_string(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_bytes([4u8; 16]),
                path: "crate".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::from_bytes([5u8; 16]),
        }),
        owner,
        node_id(7),
        1_700_000_000_000,
        1_700_000_000_000,
        Some(b"dedup".to_vec()),
    );
    let bytes = record.to_bytes().unwrap();
    assert_eq!(JobRecord::from_bytes(&bytes).unwrap(), record);
}

#[test]
fn rejects_short_record() {
    // A predecessor-format record must fail loudly, never decode with defaults.
    let record = probe_record(JobId::from_bytes([6u8; 16]), 1_700_000_000_000);
    let bytes = record.to_bytes().unwrap();
    let retention = postcard::to_allocvec(&record.retention_ms).unwrap();
    let digest = postcard::to_allocvec(&Option::<[u8; 32]>::None).unwrap();
    let mode = postcard::to_allocvec(&WorkspaceMode::Existing).unwrap();

    for trim in [
        retention.len(),
        retention.len() + digest.len(),
        retention.len() + digest.len() + mode.len(),
    ] {
        assert!(JobRecord::from_bytes(&bytes[..bytes.len() - trim]).is_err());
    }
}

fn submission() -> SubmissionId {
    SubmissionId([3u8; 32])
}

fn target() -> ExecutionTargetId {
    ExecutionTargetId {
        node_id: node_id(9),
        executor_kind: "docker".to_string(),
    }
}

fn family() -> JobFamilyId {
    JobFamilyId {
        submission_id: submission(),
        request_digest: [1u8; 32],
    }
}

fn placement() -> PlacementRef {
    PlacementRef {
        strategy_id: Ulid::from_bytes([9u8; 16]),
        shard: 5,
    }
}

/// Owns what a borrowed [`HolderView`] points at: node 1 holds the family,
/// node 2 is a member that never held it, node 9 is the execution target.
struct LocalView {
    members: Vec<NodeId>,
    holders: Vec<NodeId>,
}

impl LocalView {
    fn new() -> Self {
        Self {
            members: vec![node_id(1), node_id(2), node_id(9)],
            holders: vec![node_id(1)],
        }
    }

    /// The view after the family moved off its former holder.
    fn moved() -> Self {
        Self {
            holders: vec![node_id(2)],
            ..Self::new()
        }
    }

    fn context(&self) -> JobRecordContext<'_> {
        JobRecordContext {
            view: HolderView {
                members: &self.members,
                holders: &self.holders,
            },
            ..JobRecordContext::new(RealmId([8u8; 32]), family(), placement())
        }
    }
}

fn envelope(record: JobFamilyRecord, seed: u8) -> JobRecordEnvelope {
    JobRecordEnvelope::sign(RealmId([8u8; 32]), record, &secret(seed)).expect("record signs")
}

fn sample_output() -> OutputObject {
    OutputObject {
        node_id: node_id(1),
        bucket: "dest".to_string(),
        key: "out/report.txt".to_string(),
        version_id: Ulid::from_bytes([4u8; 16]),
        execution_id: Ulid::from_bytes([13u8; 16]),
        container_path: "/out/report.txt".to_string(),
        size: 12,
        digest: Some("aa".repeat(32)),
    }
}

fn sample_resources() -> EffectiveResources {
    EffectiveResources {
        cpu_cores: 4,
        ram_bytes: 8 * 1024 * 1024 * 1024,
        disk_bytes: 32 * 1024 * 1024 * 1024,
        max_walltime_ms: 3_600_000,
        preemptible: false,
    }
}

fn sample_admission() -> JobAdmissionRecord {
    JobAdmissionRecord {
        submission_id: submission(),
        request_digest: [1u8; 32],
        job_id: JobId::from_bytes([6u8; 16]),
        group_id: Ulid::from_bytes([7u8; 16]),
        admitting_node_id: node_id(1),
        membership_generation: 4,
        resources: sample_resources(),
        admitted_at_ms: 1_700_000_000_000,
    }
}

fn sample_spec() -> LogicalJobSpec {
    LogicalJobSpec {
        submission_id: submission(),
        job_id: JobId::from_bytes([6u8; 16]),
        origin_node_id: node_id(1),
        ingress_node_id: node_id(1),
        realm_id: RealmId([8u8; 32]),
        group_id: Ulid::from_bytes([7u8; 16]),
        created_by: user(8, 2),
        created_at_ms: 1_700_000_000_000,
        retention_ms: DEFAULT_JOB_RETENTION_MS,
        payload: ExecutionSpec {
            group_id: Ulid::from_bytes([7u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        },
        request_digest: [1u8; 32],
        spec_digest: [0u8; 32],
        resources: sample_resources(),
        retry: JobRetryPolicy {
            max_launches_per_witness: 3,
        },
        admission: sample_admission(),
        captured_inputs: Vec::new(),
        output_policies: Vec::new(),
        placement: placement(),
    }
    .store_digest()
    .expect("spec digest stored")
}

fn spec_digest() -> [u8; 32] {
    sample_spec().spec_digest
}

fn sample_claim() -> SubmissionClaim {
    SubmissionClaim {
        submission_id: submission(),
        job_id: JobId::from_bytes([6u8; 16]),
        request_digest: [1u8; 32],
        spec_digest: spec_digest(),
        committing_node_id: node_id(1),
        accepted_at_ms: 1_700_000_000_000,
    }
}

fn sample_budget() -> WitnessBudgetRecord {
    WitnessBudgetRecord {
        submission_id: submission(),
        request_digest: [1u8; 32],
        scheduler_node_id: node_id(1),
        source_spec_digest: spec_digest(),
        max_launches: 3,
    }
}

fn sample_launch() -> LaunchIntent {
    LaunchIntent {
        launch_id: Ulid::from_bytes([10u8; 16]),
        submission_id: submission(),
        request_digest: [1u8; 32],
        job_id: JobId::from_bytes([6u8; 16]),
        scheduler_node_id: node_id(1),
        scheduler_seq: 0,
        witness_placement: placement(),
        holder_generation: 11,
        target: target(),
        inputs: Vec::new(),
        output_policies: Vec::new(),
        plan_digest: [12u8; 32],
        spec_digest: spec_digest(),
        created_at_ms: 1_700_000_000_000,
    }
}

fn sample_receipt() -> ExecutionReceipt {
    ExecutionReceipt {
        execution_id: Ulid::from_bytes([13u8; 16]),
        physical_job_id: JobId::from_bytes([14u8; 16]),
        launch_id: Ulid::from_bytes([10u8; 16]),
        launch_digest: sample_launch().digest().expect("launch digests"),
        submission_id: submission(),
        request_digest: [1u8; 32],
        job_id: JobId::from_bytes([6u8; 16]),
        executor_node_id: node_id(9),
        target: target(),
        spec_digest: spec_digest(),
        membership_generation: 4,
        subject_generation: 2,
        subject_digest: [15u8; 32],
        accepted_at_ms: 1_700_000_000_000,
    }
}

fn output_record() -> ExecutionOutputRecord {
    ExecutionOutputRecord {
        execution_id: Ulid::from_bytes([13u8; 16]),
        submission_id: submission(),
        request_digest: [1u8; 32],
        job_id: JobId::from_bytes([6u8; 16]),
        executor_node_id: node_id(9),
        spec_digest: spec_digest(),
        receipt_digest: sample_receipt().digest().expect("receipt digests"),
        outputs: OutputSet::canonical(vec![sample_output()]).expect("canonical outputs"),
        committed_at_ms: 1_700_000_001_000,
    }
}

fn sample_update() -> ExecutionUpdate {
    ExecutionUpdate {
        execution_id: Ulid::from_bytes([13u8; 16]),
        submission_id: submission(),
        request_digest: [1u8; 32],
        executor_node_id: node_id(9),
        sequence: 0,
        previous_digest: sample_receipt().digest().expect("receipt digests"),
        state: PhysicalExecutionState::Running,
        observed_at_ms: 1_700_000_001_000,
        result: None,
    }
}

/// Terminal success naming the durable output record it depends on.
fn success_update() -> ExecutionUpdate {
    ExecutionUpdate {
        sequence: 1,
        previous_digest: sample_update().digest().expect("update digests"),
        state: PhysicalExecutionState::Succeeded,
        observed_at_ms: 1_700_000_002_000,
        result: Some(PhysicalExecutionResult {
            exit_code: Some(0),
            output_digest: Some(output_record().digest().expect("output digests")),
            message: None,
            stdout: None,
            stderr: None,
        }),
        ..sample_update()
    }
}

fn sample_cancel() -> JobCancelRecord {
    JobCancelRecord {
        cancel_id: Ulid::from_bytes([17u8; 16]),
        submission_id: submission(),
        request_digest: [1u8; 32],
        job_id: JobId::from_bytes([6u8; 16]),
        spec_digest: spec_digest(),
        requested_by: user(8, 2),
        authority: CancelAuthority::Submitter,
        requested_at_ms: 1_700_000_002_000,
    }
}

fn sample_projection() -> JobProjection {
    JobProjection {
        submission_id: submission(),
        request_digest: [1u8; 32],
        canonical_job_id: JobId::from_bytes([6u8; 16]),
        aliases: vec![JobId::from_bytes([6u8; 16]), JobId::from_bytes([18u8; 16])],
        state: LogicalJobState::Succeeded,
        canonical_execution_id: Some(Ulid::from_bytes([13u8; 16])),
        executions: vec![ProjectedExecution {
            execution_id: Ulid::from_bytes([13u8; 16]),
            executor_node_id: node_id(9),
            state: PhysicalExecutionState::Succeeded,
            role: ExecutionRole::Canonical,
            started_at_ms: Some(1_700_000_000_500),
            observed_at_ms: Some(1_700_000_001_000),
            result: Some(PhysicalExecutionResult {
                exit_code: Some(0),
                output_digest: Some([16u8; 32]),
                message: None,
                stdout: None,
                stderr: None,
            }),
        }],
        outputs: OutputSet::canonical(vec![sample_output()]).expect("canonical outputs"),
        cancel_requested: false,
    }
}

fn reencode<T: Serialize + serde::de::DeserializeOwned>(value: &T) -> T {
    postcard::from_bytes(&postcard::to_allocvec(value).unwrap()).unwrap()
}

fn wire_digest<T: Serialize>(value: &T) -> String {
    let bytes = postcard::to_allocvec(value).unwrap();
    hex::encode(blake3::hash(&bytes).as_bytes())
}

#[test]
fn roundtrips_records() {
    assert_eq!(reencode(&sample_output()), sample_output());
    assert_eq!(reencode(&sample_spec()), sample_spec());
    assert_eq!(reencode(&sample_admission()), sample_admission());
    assert_eq!(reencode(&sample_claim()), sample_claim());
    assert_eq!(reencode(&sample_budget()), sample_budget());
    assert_eq!(reencode(&sample_launch()), sample_launch());
    assert_eq!(reencode(&sample_receipt()), sample_receipt());
    assert_eq!(reencode(&sample_update()), sample_update());
    assert_eq!(reencode(&output_record()), output_record());
    assert_eq!(reencode(&sample_cancel()), sample_cancel());
    assert_eq!(reencode(&sample_projection()), sample_projection());
    let signed = envelope(JobFamilyRecord::Cancel(sample_cancel()), 1);
    assert_eq!(reencode(&signed), signed);
}

#[test]
fn canonical_encodings() {
    // Golden wire digests: a change here must be a deliberate format change.
    let digests = [
        wire_digest(&sample_output()),
        wire_digest(&sample_spec()),
        wire_digest(&sample_admission()),
        wire_digest(&sample_claim()),
        wire_digest(&sample_budget()),
        wire_digest(&sample_launch()),
        wire_digest(&sample_receipt()),
        wire_digest(&sample_update()),
        wire_digest(&output_record()),
        wire_digest(&sample_cancel()),
        wire_digest(&sample_projection()),
    ];
    assert_eq!(
        digests,
        [
            "2e3adfb3440145e2f0acf7dcac006c00cdbccb0fa8df37b935fc0ba4055993d2",
            "77468e0780d1ea7a34f8191e280a3b6102255a5bdb05b9890cc0f03b40695979",
            "1c4dc854b3931565ff75fafec7a24673cc03c1989516f9b46dc93a093ec2bfeb",
            "1ca48ad6fc1652c190e1808e565f7794fe08f32ac87b4d9e440447d87e2c3b93",
            "db3657f9c0d342c3291b32f42bbadf757a7356030786a173cf8c569f38695c86",
            "ded9dc94e1d65f2502e30dbcf8a5c4d2e588922d39790e451b6849f6f14ecd62",
            "3c6a1f1bfa8d44ffb88a58a76e1d792f1aeaf5545a5c837a100ea0b4921f6fbe",
            "d0797260ea757ae5d4ce2c06591444819295ea58780c29fe900e699212682f3c",
            "0b94f899dc1e80849aa991ed99e554da52560b7b68984a276a3863643c92e57e",
            "f69a68ef56b007a54533ebdb696b806e43b4cf04a75fdb453449d22190853310",
            "bb342189b2c25f4c3c70813b14199b2e1c0fc2b54e8869e05fff02957ffc5d79",
        ]
    );
    assert_eq!(postcard::to_allocvec(&submission()).unwrap(), vec![3u8; 32]);
}

#[test]
fn derives_submission_id() {
    let caller = user(1, 2);
    let other = user(1, 3);
    assert_eq!(
        SubmissionId::keyed(caller, b"key"),
        SubmissionId::keyed(caller, b"key")
    );
    assert_ne!(
        SubmissionId::keyed(caller, b"key"),
        SubmissionId::keyed(other, b"key")
    );
    // A different realm under one user ulid is a different family.
    assert_ne!(
        SubmissionId::keyed(caller, b"key"),
        SubmissionId::keyed(user(9, 2), b"key")
    );
    // Length prefixing keeps a shifted key from colliding with a longer one.
    assert_ne!(
        SubmissionId::keyed(caller, b"ab"),
        SubmissionId::keyed(caller, b"a")
    );
    assert_ne!(
        SubmissionId::unkeyed(Ulid::from_bytes([1u8; 16])),
        SubmissionId::unkeyed(Ulid::from_bytes([2u8; 16]))
    );
}

#[test]
fn rejects_empty_retry() {
    assert_eq!(
        JobRetryPolicy {
            max_launches_per_witness: 0
        }
        .validate(),
        Err(JobContractError::EmptyRetry)
    );
    assert!(
        JobRetryPolicy {
            max_launches_per_witness: 1
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn admits_stored_launch() {
    let budget = sample_budget();
    assert_eq!(budget.admits(&sample_launch()), Ok(()));

    let mut exhausted = sample_launch();
    exhausted.scheduler_seq = budget.max_launches;
    assert_eq!(
        budget.admits(&exhausted),
        Err(JobContractError::BudgetExhausted {
            sequence: budget.max_launches,
            max_launches: budget.max_launches,
        })
    );

    let mut drifted = sample_launch();
    drifted.spec_digest = [99u8; 32];
    assert_eq!(budget.admits(&drifted), Err(JobContractError::SpecMismatch));

    let mut foreign = sample_launch();
    foreign.scheduler_node_id = node_id(2);
    assert_eq!(
        budget.admits(&foreign),
        Err(JobContractError::BudgetMismatch)
    );
}

#[test]
fn orders_canonical_records() {
    // Selection must not move with a timestamp or the committing node.
    let mut restamped = sample_claim();
    restamped.accepted_at_ms = u64::MAX;
    restamped.committing_node_id = node_id(3);
    assert_eq!(restamped.order_key(), sample_claim().order_key());

    let mut alias = sample_claim();
    alias.job_id = JobId::from_bytes([2u8; 16]);
    assert_ne!(alias.order_key(), sample_claim().order_key());

    let execution = Ulid::from_bytes([13u8; 16]);
    assert_eq!(
        canonical_execution_key(submission(), [1u8; 32], execution),
        canonical_execution_key(submission(), [1u8; 32], execution)
    );
    assert_ne!(
        canonical_execution_key(submission(), [1u8; 32], execution),
        canonical_execution_key(submission(), [1u8; 32], Ulid::from_bytes([14u8; 16]))
    );
    assert_ne!(
        canonical_execution_key(submission(), [1u8; 32], execution),
        canonical_execution_key(submission(), [2u8; 32], execution)
    );
}

#[test]
fn digest_excludes_self() {
    // The one self-referential field is zeroed, never omitted.
    let spec = sample_spec();
    let mut restamped = spec.clone();
    restamped.spec_digest = [77u8; 32];
    assert_eq!(spec.digest().unwrap(), restamped.digest().unwrap());
    assert_eq!(spec.verify_digest(), Ok(()));
    assert_eq!(
        restamped.verify_digest(),
        Err(JobRecordError::DigestMismatch)
    );
    let mut moved = spec.clone();
    moved.created_at_ms += 1;
    assert_ne!(moved.digest().unwrap(), spec.digest().unwrap());
}

#[test]
fn domains_stay_distinct() {
    // `tag || body` is only unambiguous while no tag prefixes another.
    let domains = [
        JOB_SPEC_DOMAIN,
        JOB_CLAIM_DOMAIN,
        JOB_BUDGET_DOMAIN,
        JOB_LAUNCH_DOMAIN,
        JOB_RECEIPT_DOMAIN,
        JOB_UPDATE_DOMAIN,
        JOB_OUTPUT_DOMAIN,
        JOB_CANCEL_DOMAIN,
        JOB_ENVELOPE_DOMAIN,
    ];
    for (index, left) in domains.iter().enumerate() {
        for right in domains.iter().skip(index + 1) {
            assert!(!left.starts_with(right) && !right.starts_with(left));
        }
    }
}

#[test]
fn rejects_forged_author() {
    let view = LocalView::new();
    let record = JobFamilyRecord::Spec(Box::new(sample_spec()));
    assert_eq!(
        envelope(record.clone(), 1).verify(&view.context()),
        Ok(RecordVerdict::Authentic)
    );
    // A relay restating the record signs with its own key and is refused.
    assert_eq!(
        envelope(record.clone(), 2).verify(&view.context()),
        Err(JobRecordError::WrongPublisher(JobRecordKind::Spec))
    );
    let mut restated = envelope(record, 2);
    restated.published_by = node_id(1);
    assert_eq!(
        restated.verify(&view.context()),
        Err(JobRecordError::BadSignature)
    );
}

#[test]
fn rejects_forged_fields() {
    let view = LocalView::new();
    let mut forged = envelope(JobFamilyRecord::Claim(sample_claim()), 1);
    let JobFamilyRecord::Claim(claim) = &mut forged.record else {
        unreachable!("claim record")
    };
    claim.accepted_at_ms = 0;
    assert_eq!(
        forged.verify(&view.context()),
        Err(JobRecordError::BadSignature)
    );
}

#[test]
fn relay_keeps_publisher() {
    // Replication is transport: the envelope crosses a holder unchanged.
    let view = LocalView::new();
    let spec = sample_spec();
    let checked = JobRecordContext {
        spec: Some(&spec),
        ..view.context()
    };
    let signed = envelope(JobFamilyRecord::Claim(sample_claim()), 1);
    let relayed = reencode(&signed);
    assert_eq!(relayed.published_by, node_id(1));
    assert_eq!(relayed.verify(&checked), Ok(RecordVerdict::Authentic));
    assert_eq!(relayed.key(), signed.key());
}

#[test]
fn replay_is_identical() {
    let first = envelope(JobFamilyRecord::Budget(sample_budget()), 1);
    let second = envelope(JobFamilyRecord::Budget(sample_budget()), 1);
    assert_eq!(first.key(), second.key());
    assert_eq!(first.digest().unwrap(), second.digest().unwrap());
}

#[test]
fn detects_byte_conflict() {
    let first = sample_update();
    let mut second = sample_update();
    second.observed_at_ms += 1;
    assert_eq!(
        JobFamilyRecord::Update(Box::new(first.clone())).key(),
        JobFamilyRecord::Update(Box::new(second.clone())).key()
    );
    assert_ne!(first.digest().unwrap(), second.digest().unwrap());
}

#[test]
fn rejects_wrong_family() {
    let view = LocalView::new();
    let mut foreign = sample_claim();
    foreign.request_digest = [44u8; 32];
    assert_eq!(
        envelope(JobFamilyRecord::Claim(foreign), 1).verify(&view.context()),
        Err(JobRecordError::FamilyMismatch)
    );
}

#[test]
fn rejects_wrong_realm() {
    let view = LocalView::new();
    let record = JobFamilyRecord::Claim(sample_claim());
    let elsewhere = JobRecordEnvelope::sign(RealmId([99u8; 32]), record, &secret(1)).unwrap();
    assert_eq!(
        elsewhere.verify(&view.context()),
        Err(JobRecordError::RealmMismatch)
    );
    let mut spec = sample_spec();
    spec.realm_id = RealmId([99u8; 32]);
    let spec = spec.store_digest().unwrap();
    assert_eq!(
        envelope(JobFamilyRecord::Spec(Box::new(spec)), 1).verify(&view.context()),
        Err(JobRecordError::RealmMismatch)
    );
}

#[test]
fn rejects_foreign_placement() {
    let view = LocalView::new();
    let mut spec = sample_spec();
    spec.placement = PlacementRef::NIL;
    let spec = spec.store_digest().unwrap();
    assert_eq!(
        envelope(JobFamilyRecord::Spec(Box::new(spec)), 1).verify(&view.context()),
        Err(JobRecordError::PlacementMismatch)
    );
}

#[test]
fn binds_receipt_launch() {
    let view = LocalView::new();
    let launch = sample_launch();
    let spec = sample_spec();
    let checked = JobRecordContext {
        spec: Some(&spec),
        launch: Some(&launch),
        ..view.context()
    };
    let signed = envelope(JobFamilyRecord::Receipt(Box::new(sample_receipt())), 9);
    assert_eq!(signed.verify(&checked), Ok(RecordVerdict::Authentic));

    let mut drifted = sample_receipt();
    drifted.launch_digest = [1u8; 32];
    assert_eq!(
        envelope(JobFamilyRecord::Receipt(Box::new(drifted)), 9).verify(&checked),
        Err(JobRecordError::EvidenceMismatch(JobRecordKind::Launch))
    );
}

#[test]
fn bounds_stored_budget() {
    let view = LocalView::new();
    let budget = sample_budget();
    let spec = sample_spec();
    let checked = JobRecordContext {
        spec: Some(&spec),
        budget: Some(&budget),
        ..view.context()
    };
    assert_eq!(
        envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1).verify(&checked),
        Ok(RecordVerdict::Authentic)
    );
    let mut beyond = sample_launch();
    beyond.scheduler_seq = budget.max_launches;
    assert_eq!(
        envelope(JobFamilyRecord::Launch(Box::new(beyond)), 1).verify(&checked),
        Err(JobRecordError::Contract(
            JobContractError::BudgetExhausted {
                sequence: budget.max_launches,
                max_launches: budget.max_launches,
            }
        ))
    );
}

#[test]
fn cancel_needs_spec() {
    // Cancellation authority is defined only against the stored spec.
    let view = LocalView::new();
    let signed = envelope(JobFamilyRecord::Cancel(sample_cancel()), 1);
    assert_eq!(
        signed.verify(&view.context()),
        Ok(RecordVerdict::MissingEvidence(JobRecordKind::Spec))
    );
    let spec = sample_spec();
    let checked = JobRecordContext {
        spec: Some(&spec),
        ..view.context()
    };
    assert_eq!(signed.verify(&checked), Ok(RecordVerdict::Authentic));

    let mut stranger = sample_cancel();
    stranger.requested_by = user(8, 5);
    assert_eq!(
        envelope(JobFamilyRecord::Cancel(stranger), 1).verify(&checked),
        Err(JobRecordError::Unauthorized)
    );
    stranger.authority = CancelAuthority::GroupAdmin;
    assert_eq!(
        envelope(JobFamilyRecord::Cancel(stranger), 1).verify(&checked),
        Ok(RecordVerdict::Authentic)
    );
    stranger.requested_by = user(9, 5);
    assert_eq!(
        envelope(JobFamilyRecord::Cancel(stranger), 1).verify(&checked),
        Err(JobRecordError::Unauthorized)
    );
    let mut replayed = sample_cancel();
    replayed.spec_digest = [1u8; 32];
    assert_eq!(
        envelope(JobFamilyRecord::Cancel(replayed), 1).verify(&checked),
        Err(JobRecordError::EvidenceMismatch(JobRecordKind::Spec))
    );
}

/// Every kind whose author must be a current family holder, signed by the
/// node its own payload names as author.
fn holder_records() -> Vec<(JobRecordKind, JobRecordEnvelope)> {
    vec![
        (
            JobRecordKind::Spec,
            envelope(JobFamilyRecord::Spec(Box::new(sample_spec())), 1),
        ),
        (
            JobRecordKind::Claim,
            envelope(JobFamilyRecord::Claim(sample_claim()), 1),
        ),
        (
            JobRecordKind::Budget,
            envelope(JobFamilyRecord::Budget(sample_budget()), 1),
        ),
        (
            JobRecordKind::Launch,
            envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1),
        ),
        (
            JobRecordKind::Cancel,
            envelope(JobFamilyRecord::Cancel(sample_cancel()), 1),
        ),
    ]
}

#[test]
fn rejects_non_holder() {
    // A valid realm key proves identity; only the local view grants authority.
    let view = LocalView::new();
    let spec = sample_spec();
    let checked = JobRecordContext {
        spec: Some(&spec),
        ..view.context()
    };
    for (kind, signed) in holder_records() {
        assert_eq!(signed.verify(&checked).map(|_| kind), Ok(kind));
    }

    // Node 2 is a realm member that never held this family.
    let stranger = LocalView {
        holders: vec![node_id(2)],
        ..LocalView::new()
    };
    let outsider = JobRecordContext {
        spec: Some(&spec),
        ..stranger.context()
    };
    for (kind, signed) in holder_records() {
        assert_eq!(
            signed.verify(&outsider),
            Err(JobRecordError::NotHolder(kind))
        );
    }

    // A caller that resolved no view at all grants nothing.
    let unresolved = JobRecordContext::new(RealmId([8u8; 32]), family(), placement());
    for (kind, signed) in holder_records() {
        assert_eq!(
            signed.verify(&unresolved),
            Err(JobRecordError::NotHolder(kind))
        );
    }
}

#[test]
fn rejects_former_holder() {
    // A node the map still ranks but the realm no longer lists holds nothing.
    let departed = LocalView {
        members: vec![node_id(2), node_id(9)],
        holders: vec![node_id(1)],
    };
    let spec = sample_spec();
    let checked = JobRecordContext {
        spec: Some(&spec),
        ..departed.context()
    };
    for (kind, signed) in holder_records() {
        assert_eq!(
            signed.verify(&checked),
            Err(JobRecordError::NotHolder(kind))
        );
    }
}

#[test]
fn keeps_receipted_launch() {
    // The target's signed receipt is historical authority: the launch stays
    // valid after the family moved off the scheduler that published it.
    let moved = LocalView::moved();
    let spec = sample_spec();
    let budget = sample_budget();
    let receipt = sample_receipt();
    let signed = envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1);
    let unreceipted = JobRecordContext {
        spec: Some(&spec),
        budget: Some(&budget),
        ..moved.context()
    };
    assert_eq!(
        signed.verify(&unreceipted),
        Err(JobRecordError::NotHolder(JobRecordKind::Launch))
    );
    let checked = JobRecordContext {
        receipt: Some(&receipt),
        ..unreceipted
    };
    assert_eq!(signed.verify(&checked), Ok(RecordVerdict::Authentic));

    // A receipt for another launch is not that authority.
    let mut other = sample_receipt();
    other.launch_id = Ulid::from_bytes([20u8; 16]);
    assert_eq!(
        signed.verify(&JobRecordContext {
            receipt: Some(&other),
            ..unreceipted
        }),
        Err(JobRecordError::NotHolder(JobRecordKind::Launch))
    );
}

#[test]
fn holds_missing_evidence() {
    // A dependent record arriving before its predecessor is pending, never
    // authentic and never an error that would drop it.
    let view = LocalView::new();
    let bare = view.context();
    let cases = [
        (
            envelope(JobFamilyRecord::Claim(sample_claim()), 1),
            JobRecordKind::Spec,
        ),
        (
            envelope(JobFamilyRecord::Budget(sample_budget()), 1),
            JobRecordKind::Spec,
        ),
        (
            envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1),
            JobRecordKind::Spec,
        ),
        (
            envelope(JobFamilyRecord::Receipt(Box::new(sample_receipt())), 9),
            JobRecordKind::Launch,
        ),
        (
            envelope(JobFamilyRecord::Update(Box::new(sample_update())), 9),
            JobRecordKind::Receipt,
        ),
        (
            envelope(JobFamilyRecord::Output(Box::new(output_record())), 9),
            JobRecordKind::Receipt,
        ),
        (
            envelope(JobFamilyRecord::Cancel(sample_cancel()), 1),
            JobRecordKind::Spec,
        ),
    ];
    for (signed, missing) in cases {
        assert_eq!(
            signed.verify(&bare),
            Ok(RecordVerdict::MissingEvidence(missing)),
            "{:?}",
            signed.kind()
        );
    }
    // The launch's own budget is required even once the spec is verified.
    let spec = sample_spec();
    assert_eq!(
        envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1).verify(&JobRecordContext {
            spec: Some(&spec),
            ..bare
        }),
        Ok(RecordVerdict::MissingEvidence(JobRecordKind::Budget))
    );
}

#[test]
fn verdicts_ignore_order() {
    // Every arrival order of the same valid records ends in the same verdict:
    // absent evidence is pending, complete evidence is authentic.
    let view = LocalView::new();
    let spec = sample_spec();
    let budget = sample_budget();
    let launch = sample_launch();
    let receipt = sample_receipt();
    let signed = [
        envelope(JobFamilyRecord::Claim(sample_claim()), 1),
        envelope(JobFamilyRecord::Launch(Box::new(launch.clone())), 1),
        envelope(JobFamilyRecord::Receipt(Box::new(receipt.clone())), 9),
        envelope(JobFamilyRecord::Update(Box::new(sample_update())), 9),
        envelope(JobFamilyRecord::Output(Box::new(output_record())), 9),
    ];
    for mask in 0..16u8 {
        let context = JobRecordContext {
            spec: (mask & 1 != 0).then_some(&spec),
            budget: (mask & 2 != 0).then_some(&budget),
            launch: (mask & 4 != 0).then_some(&launch),
            receipt: (mask & 8 != 0).then_some(&receipt),
            ..view.context()
        };
        for record in &signed {
            let verdict = record.verify(&context).expect("valid records never error");
            let complete = match record.kind() {
                JobRecordKind::Claim => mask & 1 != 0,
                JobRecordKind::Launch => mask & 3 == 3,
                JobRecordKind::Receipt => mask & 4 != 0,
                JobRecordKind::Update | JobRecordKind::Output => mask & 8 != 0,
                kind => unreachable!("{kind:?} is not part of this set"),
            };
            assert_eq!(
                verdict == RecordVerdict::Authentic,
                complete,
                "{:?} at mask {mask}",
                record.kind()
            );
        }
    }
}

#[test]
fn rejects_wrong_target() {
    // Only the node the scheduler named as target may receipt that launch.
    let view = LocalView::new();
    let spec = sample_spec();
    let launch = sample_launch();
    let checked = JobRecordContext {
        spec: Some(&spec),
        launch: Some(&launch),
        ..view.context()
    };
    let mut elsewhere = sample_receipt();
    elsewhere.executor_node_id = node_id(2);
    elsewhere.target = ExecutionTargetId {
        node_id: node_id(2),
        executor_kind: "docker".to_string(),
    };
    assert_eq!(
        envelope(JobFamilyRecord::Receipt(Box::new(elsewhere.clone())), 2).verify(&checked),
        Err(JobRecordError::EvidenceMismatch(JobRecordKind::Launch))
    );
    // A receipt naming another node as its own target contradicts itself.
    let mut mismatched = sample_receipt();
    mismatched.executor_node_id = node_id(2);
    assert_eq!(
        envelope(JobFamilyRecord::Receipt(Box::new(mismatched)), 2).verify(&checked),
        Err(JobRecordError::Inconsistent)
    );
    assert_eq!(
        envelope(JobFamilyRecord::Receipt(Box::new(elsewhere)), 9).verify(&checked),
        Err(JobRecordError::WrongPublisher(JobRecordKind::Receipt))
    );
}

#[test]
fn roots_update_chain() {
    let view = LocalView::new();
    let receipt = sample_receipt();
    let checked = JobRecordContext {
        receipt: Some(&receipt),
        ..view.context()
    };
    assert_eq!(
        envelope(JobFamilyRecord::Update(Box::new(sample_update())), 9).verify(&checked),
        Ok(RecordVerdict::Authentic)
    );
    // The first update must root at the exact receipt it claims to follow.
    let mut unrooted = sample_update();
    unrooted.previous_digest = [5u8; 32];
    assert_eq!(
        envelope(JobFamilyRecord::Update(Box::new(unrooted)), 9).verify(&checked),
        Err(JobRecordError::EvidenceMismatch(JobRecordKind::Receipt))
    );
    let mut foreign = sample_update();
    foreign.executor_node_id = node_id(2);
    assert_eq!(
        envelope(JobFamilyRecord::Update(Box::new(foreign)), 2).verify(&checked),
        Err(JobRecordError::EvidenceMismatch(JobRecordKind::Receipt))
    );
}

#[test]
fn accepts_local_output() {
    // A node's own fenced attempt validates its own output record while no
    // launch chain exists; the same record from anyone else stays pending.
    let view = LocalView::new();
    let local = LocalExecution {
        node_id: node_id(9),
        execution_id: output_record().execution_id,
        fence_digest: output_record().receipt_digest,
        spec_digest: output_record().spec_digest,
    };
    let signed = envelope(JobFamilyRecord::Output(Box::new(output_record())), 9);
    assert_eq!(
        signed.verify(&JobRecordContext {
            local: Some(&local),
            ..view.context()
        }),
        Ok(RecordVerdict::LocalEvidence)
    );
    let other = LocalExecution {
        execution_id: Ulid::from_bytes([21u8; 16]),
        ..local
    };
    assert_eq!(
        signed.verify(&JobRecordContext {
            local: Some(&other),
            ..view.context()
        }),
        Ok(RecordVerdict::MissingEvidence(JobRecordKind::Receipt))
    );
    // A published receipt outranks the local stand-in.
    let receipt = sample_receipt();
    assert_eq!(
        signed.verify(&JobRecordContext {
            local: Some(&local),
            receipt: Some(&receipt),
            ..view.context()
        }),
        Ok(RecordVerdict::Authentic)
    );
}

#[test]
fn rejects_forged_cancel() {
    // Selecting GroupAdmin or naming a submitter cannot create authority.
    let view = LocalView::new();
    let spec = sample_spec();
    let checked = JobRecordContext {
        spec: Some(&spec),
        ..view.context()
    };
    let mut forged = sample_cancel();
    forged.authority = CancelAuthority::GroupAdmin;
    forged.requested_by = user(8, 9);
    assert_eq!(
        envelope(JobFamilyRecord::Cancel(forged), 2).verify(&checked),
        Err(JobRecordError::NotHolder(JobRecordKind::Cancel))
    );
    let mut anonymous = sample_cancel();
    anonymous.requested_by = UserId::new(Ulid::nil(), RealmId([8u8; 32]));
    anonymous.authority = CancelAuthority::GroupAdmin;
    assert_eq!(
        envelope(JobFamilyRecord::Cancel(anonymous), 1).verify(&checked),
        Err(JobRecordError::Unauthorized)
    );
}

#[test]
fn rejects_family_replay() {
    // One authentic record replayed into another realm or family is refused
    // even when every local view would otherwise grant its publisher.
    let view = LocalView::new();
    let spec = sample_spec();
    let signed = envelope(JobFamilyRecord::Claim(sample_claim()), 1);
    let elsewhere = JobRecordContext {
        spec: Some(&spec),
        family: JobFamilyId {
            submission_id: SubmissionId([7u8; 32]),
            request_digest: [1u8; 32],
        },
        ..view.context()
    };
    assert_eq!(
        signed.verify(&elsewhere),
        Err(JobRecordError::FamilyMismatch)
    );
    let other_realm = JobRecordContext {
        spec: Some(&spec),
        realm_id: RealmId([99u8; 32]),
        ..view.context()
    };
    assert_eq!(
        signed.verify(&other_realm),
        Err(JobRecordError::RealmMismatch)
    );
}

#[test]
fn outputs_sort_canonically() {
    let first = sample_output();
    let mut second = sample_output();
    second.key = "out/a.txt".to_string();
    let sorted = OutputSet::canonical(vec![second.clone(), first.clone()]).unwrap();
    assert_eq!(
        sorted,
        OutputSet::canonical(vec![first.clone(), second.clone()]).unwrap()
    );
    assert_eq!(sorted.as_slice()[0], second);

    // A peer may not reorder or duplicate what its publisher signed.
    let unsorted = postcard::to_allocvec(&vec![first.clone(), second.clone()]).unwrap();
    assert!(postcard::from_bytes::<OutputSet>(&unsorted).is_err());
    assert_eq!(
        OutputSet::canonical(vec![first.clone(), first.clone()]),
        Err(JobRecordError::OutputOrder)
    );
    let mut anonymous = sample_output();
    anonymous.version_id = Ulid::nil();
    assert_eq!(
        OutputSet::canonical(vec![anonymous]),
        Err(JobRecordError::OutputOrder)
    );
    let many = (0..=MAX_EXECUTION_OUTPUTS)
        .map(|index| OutputObject {
            key: format!("out/{index:08}"),
            ..sample_output()
        })
        .collect();
    assert_eq!(OutputSet::canonical(many), Err(JobRecordError::OutputOrder));
}

fn execution_result(outputs: Vec<OutputObject>) -> JobResultPayload {
    JobResultPayload::Execution {
        exit_code: Some(0),
        workspace_bucket: Some("ws".to_string()),
        outputs,
        stdout: String::new(),
        stderr: String::new(),
        output_digest: None,
    }
}

#[test]
fn success_needs_identity() {
    let execution_id = sample_output().execution_id;
    assert!(
        execution_result(vec![sample_output()])
            .check_outputs(execution_id)
            .is_ok()
    );

    let mut anonymous = sample_output();
    anonymous.version_id = Ulid::nil();
    assert_eq!(
        execution_result(vec![anonymous]).check_outputs(execution_id),
        Err(JobRecordError::OutputIdentity)
    );

    // An output produced by another execution may not ride this result.
    let mut foreign = sample_output();
    foreign.execution_id = Ulid::from_bytes([14u8; 16]);
    assert_eq!(
        execution_result(vec![foreign]).check_outputs(execution_id),
        Err(JobRecordError::OutputIdentity)
    );
    assert_eq!(
        execution_result(vec![sample_output()]).check_outputs(Ulid::nil()),
        Err(JobRecordError::OutputIdentity)
    );
    assert_eq!(
        execution_result(vec![sample_output(), sample_output()]).check_outputs(execution_id),
        Err(JobRecordError::OutputIdentity)
    );
}

fn sample_control() -> AttemptControl {
    AttemptControl {
        attempt_epoch: 3,
        execution_id: Ulid::from_bytes([13u8; 16]),
        controller_generation: 1,
        bound_token: None,
        tombstone_ref: None,
        output_commits: Vec::new(),
        output_record: None,
    }
}

#[test]
fn success_needs_record() {
    // Success must observe a durable output record and a reserved version
    // for every object, so an unproven result can never commit.
    let mut control = sample_control();
    let output = sample_output();
    let mut result = execution_result(vec![output.clone()]);

    assert_eq!(
        result.proves_outputs(&control),
        Err(JobRecordError::OutputIdentity)
    );

    control.output_commits.push(OutputCommitIntent {
        node_id: output.node_id,
        bucket: output.bucket.clone(),
        key: output.key.clone(),
        version_id: output.version_id,
    });
    assert_eq!(
        result.proves_outputs(&control),
        Err(JobRecordError::MissingEvidence(JobRecordKind::Output))
    );

    let digest = [7u8; 32];
    control.output_record = Some(digest);
    assert_eq!(
        result.proves_outputs(&control),
        Err(JobRecordError::MissingEvidence(JobRecordKind::Output))
    );

    if let JobResultPayload::Execution { output_digest, .. } = &mut result {
        *output_digest = Some(digest);
    }
    assert_eq!(result.proves_outputs(&control), Ok(()));
}

#[test]
fn success_rejects_foreign() {
    // A version another writer produced under the same key is not this
    // execution's output, even when the key was reserved.
    let mut control = sample_control();
    let output = sample_output();
    control.output_commits.push(OutputCommitIntent {
        node_id: output.node_id,
        bucket: output.bucket.clone(),
        key: output.key.clone(),
        version_id: Ulid::from_bytes([9u8; 16]),
    });
    control.output_record = Some([7u8; 32]);
    let mut result = execution_result(vec![output]);
    if let JobResultPayload::Execution { output_digest, .. } = &mut result {
        *output_digest = Some([7u8; 32]);
    }

    assert_eq!(
        result.proves_outputs(&control),
        Err(JobRecordError::OutputIdentity)
    );
}

#[test]
fn empty_success_recorded() {
    // Even an empty output set is a claim: it needs its durable record.
    let mut control = sample_control();
    let mut result = execution_result(Vec::new());
    assert_eq!(
        result.proves_outputs(&control),
        Err(JobRecordError::MissingEvidence(JobRecordKind::Output))
    );
    control.output_record = Some([3u8; 32]);
    if let JobResultPayload::Execution { output_digest, .. } = &mut result {
        *output_digest = Some([3u8; 32]);
    }
    assert_eq!(result.proves_outputs(&control), Ok(()));
}

#[test]
fn reservation_is_stable() {
    // A replayed capture must reuse the reserved VersionId, never mint a second.
    let mut control = sample_control();
    let output_node = node_id(1);
    let destinations = vec![
        (output_node, "dest".to_string(), "out/a.txt".to_string()),
        (output_node, "dest".to_string(), "out/b.txt".to_string()),
    ];
    assert!(control.reserve_outputs(&destinations, Ulid::generate));
    let reserved = control.output_commits.clone();
    assert_eq!(reserved.len(), 2);
    assert!(!control.reserve_outputs(&destinations, Ulid::generate));
    assert_eq!(control.output_commits, reserved);

    let grown = vec![(output_node, "dest".to_string(), "out/c.txt".to_string())];
    assert!(control.reserve_outputs(&grown, Ulid::generate));
    assert_eq!(control.output_commits[..2], reserved[..]);
    let remote = vec![(node_id(2), "dest".to_string(), "out/a.txt".to_string())];
    assert!(control.reserve_outputs(&remote, Ulid::generate));
}

#[test]
fn binds_output_receipt() {
    let view = LocalView::new();
    let receipt = sample_receipt();
    let checked = JobRecordContext {
        receipt: Some(&receipt),
        ..view.context()
    };
    assert_eq!(
        envelope(JobFamilyRecord::Output(Box::new(output_record())), 9).verify(&checked),
        Ok(RecordVerdict::Authentic)
    );
    let mut foreign = output_record();
    foreign.outputs = OutputSet::canonical(vec![OutputObject {
        execution_id: Ulid::from_bytes([1u8; 16]),
        ..sample_output()
    }])
    .unwrap();
    assert_eq!(
        envelope(JobFamilyRecord::Output(Box::new(foreign)), 9).verify(&checked),
        Err(JobRecordError::Inconsistent)
    );
}

#[test]
fn chain_stops_gap() {
    let root = sample_receipt().digest().unwrap();
    let outputs = output_record().digest().unwrap();
    let mut gapped = success_update();
    gapped.sequence = 2;
    let records = [gapped, sample_update()];
    let chain = verify_update_chain(root, Some(outputs), &records).unwrap();
    assert_eq!(chain, vec![&sample_update()]);

    let mut unrooted = sample_update();
    unrooted.previous_digest = [0u8; 32];
    assert!(
        verify_update_chain(root, Some(outputs), &[unrooted])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn chain_stops_terminal() {
    let root = sample_receipt().digest().unwrap();
    let outputs = output_record().digest().unwrap();
    let mut trailing = sample_update();
    trailing.sequence = 2;
    trailing.previous_digest = success_update().digest().unwrap();
    let records = [sample_update(), success_update(), trailing];
    let chain = verify_update_chain(root, Some(outputs), &records).unwrap();
    assert_eq!(chain, vec![&sample_update(), &success_update()]);
}

#[test]
fn chain_requires_outputs() {
    // A success is projected only once its exact output record is durable.
    let root = sample_receipt().digest().unwrap();
    let records = [sample_update(), success_update()];
    assert_eq!(
        verify_update_chain(root, None, &records).unwrap(),
        vec![&sample_update()]
    );
    assert_eq!(
        verify_update_chain(root, Some([3u8; 32]), &records).unwrap(),
        vec![&sample_update()]
    );
    let outputs = output_record().digest().unwrap();
    assert_eq!(
        verify_update_chain(root, Some(outputs), &records)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn chain_rejects_conflict() {
    let root = sample_receipt().digest().unwrap();
    let mut rival = sample_update();
    rival.observed_at_ms += 1;
    assert_eq!(
        verify_update_chain(root, None, &[sample_update(), rival]),
        Err(JobRecordError::ChainConflict { sequence: 0 })
    );
    // An exact replay of one record is a no-op, not a conflict.
    assert_eq!(
        verify_update_chain(root, None, &[sample_update(), sample_update()])
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn key_bytes_ordered() {
    let mut keys = vec![
        JobFamilyRecord::Cancel(sample_cancel()).key(),
        JobFamilyRecord::Spec(Box::new(sample_spec())).key(),
        JobFamilyRecord::Update(Box::new(success_update())).key(),
        JobFamilyRecord::Update(Box::new(sample_update())).key(),
        JobFamilyRecord::Receipt(Box::new(sample_receipt())).key(),
    ];
    keys.sort();
    let mut encoded: Vec<[u8; JOB_RECORD_KEY_BYTES]> =
        keys.iter().map(JobRecordKey::to_bytes).collect();
    encoded.sort();
    assert_eq!(
        encoded,
        keys.iter()
            .map(JobRecordKey::to_bytes)
            .collect::<Vec<[u8; JOB_RECORD_KEY_BYTES]>>()
    );
    for key in keys {
        assert_eq!(JobRecordKey::from_bytes(&key.to_bytes()), Ok(key));
    }
    assert_eq!(
        JobRecordKey::from_bytes(&[0u8; JOB_RECORD_KEY_BYTES - 1]),
        Err(JobRecordError::MalformedKey)
    );
}

#[test]
fn bounds_result_message() {
    assert!(ResultMessage::new("m".repeat(MAX_RESULT_MESSAGE_BYTES)).is_ok());
    assert_eq!(
        ResultMessage::new("m".repeat(MAX_RESULT_MESSAGE_BYTES + 1)),
        Err(JobRecordError::MessageBytes)
    );
    let over = postcard::to_allocvec(&"m".repeat(MAX_RESULT_MESSAGE_BYTES + 1)).unwrap();
    assert!(postcard::from_bytes::<ResultMessage>(&over).is_err());
}

#[test]
fn keeps_message_tail() {
    // An overlong stream keeps its end, and a cut inside a character moves
    // forward to the next boundary rather than producing invalid UTF-8.
    assert_eq!(ResultMessage::tail(""), None);
    assert_eq!(
        ResultMessage::tail("short").map(|tail| tail.as_str().to_string()),
        Some("short".to_string())
    );
    let text = format!("{}end", "m".repeat(MAX_RESULT_MESSAGE_BYTES));
    let tail = ResultMessage::tail(&text).expect("non-empty tail");
    assert_eq!(tail.as_str().len(), MAX_RESULT_MESSAGE_BYTES);
    assert!(tail.as_str().ends_with("end"));
    // 6000 bytes of three-byte characters: the cut at 1904 is inside one.
    let wide = "€".repeat(2000);
    let tail = ResultMessage::tail(&wide).expect("non-empty tail");
    assert_eq!(tail.as_str().len(), MAX_RESULT_MESSAGE_BYTES - 1);
    assert!(wide.ends_with(tail.as_str()));
}

#[test]
fn digest_tracks_projection() {
    let projection = sample_projection();
    assert_eq!(
        projection.digest().unwrap(),
        sample_projection().digest().unwrap()
    );
    let mut succeeded = projection.clone();
    succeeded.state = LogicalJobState::Cancelled;
    assert_ne!(succeeded.digest().unwrap(), projection.digest().unwrap());
}

#[test]
fn record_key_versioned() {
    let key = job_record_key(JobId::from_bytes([9u8; 16]));
    assert!(key.starts_with(JOB_RECORD_KEY_PREFIX));
    assert_eq!(key.len(), JOB_RECORD_KEY_PREFIX.len() + 16);
}

#[test]
fn due_index_ordered() {
    let id = JobId::from_bytes([1u8; 16]);
    assert!(due_index_key(1_000, id) < due_index_key(2_000, id));
    let (ts, parsed) = parse_schedule_key(&due_index_key(1_234, id)).unwrap();
    assert_eq!(ts, 1_234);
    assert_eq!(parsed, id);
}

#[test]
fn prefixes_disjoint() {
    let id = JobId::from_bytes([1u8; 16]);
    let due = due_index_key(5, id);
    let lease = lease_index_key(5, id);
    let prune = job_prune_key(5, id);
    assert!(due < lease);
    assert!(lease < prune);
    assert!(!due.starts_with(JOB_LEASE_INDEX_PREFIX));
    assert!(!lease.starts_with(JOB_PRUNE_INDEX_PREFIX));
}

#[test]
fn owner_index_newest() {
    let u = user(1, 2);
    let id = JobId::from_bytes([3u8; 16]);
    assert!(owner_index_key(u, 2_000, id) < owner_index_key(u, 1_000, id));
    assert!(owner_index_key(u, 1_000, id).starts_with(owner_index_prefix(u).as_ref()));
}

#[test]
fn owner_key_roundtrips() {
    let u = user(5, 9);
    let ts = 1_700_000_000_000u64;
    let id = JobId::from_bytes([8u8; 16]);
    let key = owner_index_key(u, ts, id);
    assert_eq!(parse_owner_key(&key).unwrap(), (u, ts, id));
    assert_eq!(job_owner_cursor(ts, id).as_slice(), &key[48..72]);
}

#[test]
fn owner_index_scoped() {
    let a = user(1, 2);
    let b = user(1, 3);
    let id = JobId::from_bytes([4u8; 16]);
    let ka = owner_index_key(a, 1_000, id);
    assert!(!ka.starts_with(owner_index_prefix(b).as_ref()));
}
