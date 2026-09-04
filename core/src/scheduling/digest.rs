//! The stored plan digest. It covers every value the plan was made from, so a
//! launch that replays it can be checked against the same evidence.

use crate::compute::ResourceEnvelope;
use crate::scheduling::cost::InputRoute;
use crate::scheduling::inputs::{PlanRequest, TargetCandidate, TargetScore};

pub const PLAN_DIGEST_DOMAIN: &[u8] = b"aruna-execution-plan-v1";

/// Digest over the request identity, the pinned inputs and their chosen
/// sources, the target's eligibility values and stored subject, and the score.
/// `routes` is parallel to `request.inputs`, which is canonically ordered.
pub fn plan_digest(
    request: &PlanRequest,
    candidate: &TargetCandidate,
    routes: &[InputRoute],
    score: &TargetScore,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(PLAN_DIGEST_DOMAIN);
    hasher.update(&request.submission_id.0);
    hasher.update(&request.request_digest);
    hasher.update(&request.spec_digest);

    let resources = &request.resources;
    hasher.update(&u64::from(resources.cpu_cores).to_le_bytes());
    hasher.update(&resources.ram_bytes.to_le_bytes());
    hasher.update(&resources.disk_bytes.to_le_bytes());
    hasher.update(&resources.max_walltime_ms.to_le_bytes());
    flag(&mut hasher, resources.preemptible);
    field(&mut hasher, request.executor_constraint.as_deref());
    count(&mut hasher, request.required_labels.len());
    for label in &request.required_labels {
        field(&mut hasher, Some(&label.key));
        field(&mut hasher, Some(&label.value));
    }
    hasher.update(&[request.staging as u8, request.network as u8]);

    count(&mut hasher, request.inputs.len());
    for (input, route) in request.inputs.iter().zip(routes) {
        field(&mut hasher, Some(&input.destination_key));
        hasher.update(&input.version_id.to_bytes());
        hasher.update(&input.blake3);
        hasher.update(&input.bytes.to_le_bytes());
        count(&mut hasher, input.policies.len());
        for policy in &input.policies {
            hasher.update(&policy.policy_id.to_bytes());
            hasher.update(&policy.digest);
        }
        match route.source_node_id {
            Some(node_id) => {
                hasher.update(&[1]);
                hasher.update(node_id.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        hasher.update(&route.transfer_ms.to_le_bytes());
        hasher.update(&route.transfer_bytes.to_le_bytes());
        flag(&mut hasher, route.known_link);
    }
    count(&mut hasher, request.output_policies.len());
    for policy in &request.output_policies {
        hasher.update(&policy.policy_id.to_bytes());
        hasher.update(&policy.digest);
    }

    let capability = &candidate.capability;
    hasher.update(candidate.node_id.as_bytes());
    field(&mut hasher, Some(&capability.kind));
    field(&mut hasher, Some(candidate.node_kind.label()));
    hasher.update(&capability.subject_digest);
    hasher.update(&capability.subject.generation.to_le_bytes());
    for value in [
        candidate.active,
        candidate.compute_draining,
        candidate.group_allowed,
        capability.file_staging,
        capability.direct_s3,
        capability.s3_mount,
        capability.network_policy,
        capability.policy_draining,
    ] {
        flag(&mut hasher, value);
    }
    let ResourceEnvelope {
        max_cpu_cores,
        max_ram_bytes,
        max_disk_bytes,
        max_concurrent,
    } = capability.limits;
    ceiling(&mut hasher, max_cpu_cores.map(u64::from));
    ceiling(&mut hasher, max_ram_bytes);
    ceiling(&mut hasher, max_disk_bytes);
    ceiling(&mut hasher, max_concurrent.map(u64::from));

    hasher.update(&score.estimated_transfer_ms.to_le_bytes());
    hasher.update(&score.transfer_bytes.to_le_bytes());
    hasher.update(&score.availability_pressure_permille.to_le_bytes());
    hasher.update(&score.node_load_permille.to_le_bytes());
    hasher.update(&score.compute_priority_inverse.to_le_bytes());
    hasher.update(&score.unknown_link_count.to_le_bytes());
    *hasher.finalize().as_bytes()
}

/// Length-prefixed optional field, so two fields cannot collide across their
/// encoding boundary.
fn field(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn count(hasher: &mut blake3::Hasher, value: usize) {
    hasher.update(&(value as u64).to_le_bytes());
}

fn flag(hasher: &mut blake3::Hasher, value: bool) {
    hasher.update(&[u8::from(value)]);
}

fn ceiling(hasher: &mut blake3::Hasher, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value.to_le_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}
