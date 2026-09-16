use super::*;

mod config;
mod group;
mod placement;
mod realm;
mod transition;
mod user;

impl AdminDocumentState {
    pub fn apply(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminApplyStatus, AdminDocumentError> {
        if event.target != self.target {
            return Err(AdminDocumentError::TargetMismatch);
        }
        // Duplicate event IDs cannot overwrite the first event, so equivocation gains nothing.
        // Admission, rather than reduction, retains the evidence.
        if self.applied_event_ids.contains(&event.event_id) {
            return Ok(AdminApplyStatus::Duplicate);
        }
        let all_paths_stale = !matches!(
            &event.op,
            AdminDocumentOperation::PlacementBindingAppended { .. }
                | AdminDocumentOperation::HandleRangeGranted { .. }
                | AdminDocumentOperation::BandPoolAssigned { .. }
                | AdminDocumentOperation::ConfigTokenRevoked { .. }
                | AdminDocumentOperation::CandidateMapPublished { .. }
                | AdminDocumentOperation::ConfigActivationsInitialized { .. }
                | AdminDocumentOperation::ConfigTransitionStarted { .. }
                | AdminDocumentOperation::TransitionBarrierReported { .. }
                | AdminDocumentOperation::TransitionProofSubmitted { .. }
                | AdminDocumentOperation::ConfigTransitionAborted { .. }
                | AdminDocumentOperation::TransitionBucketForced { .. }
                | AdminDocumentOperation::TransitionStallReported { .. }
                | AdminDocumentOperation::TransitionDrainReported { .. }
        ) && operation_paths(&event.op)
            .iter()
            .all(|path| self.event_path_stale(event, path));
        let apply_status = self.apply_event(event)?;

        if all_paths_stale {
            self.applied_event_ids.insert(event.event_id);
            self.clock.advance(event.origin_node_id, event.origin_seq);
            return Ok(AdminApplyStatus::StaleOriginSequence);
        }

        self.clock.advance(event.origin_node_id, event.origin_seq);
        if apply_status != AdminApplyStatus::Redundant {
            self.applied_event_ids.insert(event.event_id);
        }
        Ok(apply_status)
    }

    fn apply_event(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminApplyStatus, AdminDocumentError> {
        match &event.target {
            AdminDocumentTarget::Group { group_id } => self.apply_group(event, group_id),
            AdminDocumentTarget::Realm { .. } => self.apply_realm(event),
            AdminDocumentTarget::User { .. } => self.apply_user(event),
            AdminDocumentTarget::RealmConfig { realm_id } => self.apply_config(event, realm_id),
        }
    }
}
