use super::*;

pub(crate) const NOW_MS: u64 = 1_700_000_000_000;

pub(crate) fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(crate) fn subject(node_id: NodeId, location: &str) -> PlacementSubject {
    PlacementSubject {
        node_id,
        generation: 3,
        location: location.to_string(),
        labels: BTreeMap::new(),
        executor_kind: None,
        local_to_controller: true,
    }
}

pub(crate) fn candidate(node_id: NodeId, kind: &str) -> TargetCandidate {
    candidate_at(node_id, kind, "eu")
}

pub(crate) fn candidate_at(node_id: NodeId, kind: &str, location: &str) -> TargetCandidate {
    let mut capability = ExecutorCapability::new(kind.to_string(), subject(node_id, location))
        .expect("subject is valid");
    capability.file_staging = true;
    TargetCandidate {
        node_id,
        node_kind: RealmNodeKind::Server,
        active: true,
        compute_draining: false,
        group_allowed: true,
        capability,
        load_permille: Some(100),
    }
}

/// Refreshes the advertised digest after a fixture changed the subject.
pub(crate) fn refresh_digest(candidate: &mut TargetCandidate) {
    candidate.capability.subject_digest = candidate
        .capability
        .subject
        .digest()
        .expect("subject is valid");
}

pub(crate) fn holder(node_id: NodeId, location: &str) -> InputHolder {
    InputHolder {
        node_id,
        subject: subject(node_id, location),
    }
}

pub(crate) fn resolved_input(key: &str, seed: u8) -> ResolvedInput {
    let version = Ulid::from_bytes([seed; 16]);
    ResolvedInput {
        destination_key: key.to_string(),
        source: VersionedObjectArn {
            realm_id: crate::structs::RealmId::from_bytes([1u8; 32]),
            node_id: node(1),
            bucket: "bucket".to_string(),
            key: key.to_string(),
            version,
        },
        version_id: version,
        blake3: [seed; 32],
        bytes: 1_000,
        policies: Vec::new(),
        holders: Vec::new(),
    }
}

pub(crate) fn request(inputs: Vec<ResolvedInput>) -> PlanRequest {
    PlanRequest {
        submission_id: SubmissionId([7u8; 32]),
        request_digest: [8u8; 32],
        spec_digest: [9u8; 32],
        admitted: true,
        resources: EffectiveResources {
            cpu_cores: 2,
            ram_bytes: 1_024,
            disk_bytes: 2_048,
            max_walltime_ms: 60_000,
            preemptible: false,
        },
        executor_constraint: None,
        required_labels: Vec::new(),
        staging: StagingMode::Files,
        network: NetworkAccess::Isolated,
        session: false,
        inputs,
        output_policies: Vec::new(),
        policies: BTreeMap::new(),
        now_ms: NOW_MS,
    }
}

pub(crate) fn policy(seed: u8, selector: PlacementSelector) -> VerifiedPolicy {
    VerifiedPolicy::verify(
        PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            format!("policy-{seed}"),
            vec![selector],
        )
        .expect("policy is valid"),
    )
    .expect("policy is canonical")
}

pub(crate) fn selector() -> PlacementSelector {
    PlacementSelector {
        node_id: None,
        location: None,
        labels: Vec::new(),
        executor_kind: None,
    }
}

/// Registers `policy` as locally verified and returns its ref.
pub(crate) fn known(request: &mut PlanRequest, policy: &VerifiedPolicy) -> PlacementPolicyRef {
    request.policies.insert(
        policy.policy().policy_id,
        PolicyResolution::Known(policy.clone()),
    );
    policy.policy_ref()
}

pub(crate) fn config(links: Vec<LocationLink>) -> RealmComputeConfig {
    RealmComputeConfig {
        links,
        ..Default::default()
    }
}

pub(crate) fn link(from: &str, to: &str, bandwidth: u64) -> LocationLink {
    LocationLink {
        from: from.to_string(),
        to: to.to_string(),
        bandwidth_bytes_per_sec: bandwidth,
    }
}
