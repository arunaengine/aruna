use aruna_core::compute::{AttemptPhase, ReconcileEvidence, ResumePoint};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryAction {
    Observe,
    RetrySame,
    Cleanup,
    Retire,
    Park,
}

pub fn recovery_action(evidence: &ReconcileEvidence) -> RecoveryAction {
    match evidence {
        ReconcileEvidence::Adoptable(adoptable) => {
            match (&adoptable.status.phase, adoptable.resume) {
                (AttemptPhase::Submitted, ResumePoint::Submit) => RecoveryAction::RetrySame,
                (_, ResumePoint::Stage | ResumePoint::Unsuspend) => RecoveryAction::RetrySame,
                _ => RecoveryAction::Observe,
            }
        }
        ReconcileEvidence::Unadoptable(artifact) if artifact.exact_identity => {
            RecoveryAction::Cleanup
        }
        ReconcileEvidence::Unadoptable(_) | ReconcileEvidence::Unavailable(_) => {
            RecoveryAction::Park
        }
        ReconcileEvidence::Absent => RecoveryAction::RetrySame,
        ReconcileEvidence::Tombstoned(_) => RecoveryAction::Retire,
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::{
        AdoptableEvidence, ArtifactEvidence, AttemptStatus, BackendError, ReconcileEvidence,
        ResumePoint, TombstoneEvidence,
    };

    use super::*;

    #[test]
    fn maps_all_evidence() {
        let status = AttemptStatus {
            phase: AttemptPhase::Submitted,
            backend_ref: "attempt".to_string(),
            started_at_ms: None,
            finished_at_ms: None,
        };
        let exact = ArtifactEvidence {
            artifact_kind: "helper".to_string(),
            backend_ref: None,
            observed_epoch: Some(1),
            observed_generation: Some(1),
            exact_identity: true,
            multiple: false,
            foreign: false,
        };
        assert_eq!(
            recovery_action(&ReconcileEvidence::Adoptable(AdoptableEvidence {
                status,
                resume: ResumePoint::Submit,
            })),
            RecoveryAction::RetrySame
        );
        assert_eq!(
            recovery_action(&ReconcileEvidence::Unadoptable(exact)),
            RecoveryAction::Cleanup
        );
        assert_eq!(
            recovery_action(&ReconcileEvidence::Absent),
            RecoveryAction::RetrySame
        );
        assert_eq!(
            recovery_action(&ReconcileEvidence::Tombstoned(TombstoneEvidence {
                backend_ref: "tombstone".to_string(),
                attempt_epoch: 1,
            })),
            RecoveryAction::Retire
        );
        assert_eq!(
            recovery_action(&ReconcileEvidence::Unavailable(BackendError::Unavailable(
                "down".to_string(),
            ))),
            RecoveryAction::Park
        );
    }
}
