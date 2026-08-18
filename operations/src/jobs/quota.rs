//! The logical admission quota seam.

use aruna_core::structs::EffectiveResources;
use aruna_core::types::GroupId;
use thiserror::Error;

/// Why standing quota refused a new logical admission.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("group {group_id} is over its standing compute quota: {reason}")]
pub struct QuotaDenied {
    pub group_id: GroupId,
    pub reason: String,
}

/// Standing-quota decision taken before one submission is logically admitted.
/// The quota round wires the replicated demand evaluator into this seam; until
/// then admission is allowed and an observed overshoot cancels nothing.
pub fn quota_gate(group_id: GroupId, resources: &EffectiveResources) -> Result<(), QuotaDenied> {
    let _ = (group_id, resources);
    Ok(())
}
