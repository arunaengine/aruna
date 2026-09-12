use super::*;

mod config;
mod group;
mod placement;
mod realm;
mod transition;
mod user;

impl AdminDocumentReducer {
    pub fn apply(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        if event.target != self.target {
            return Err(AdminDocumentReducerError::TargetMismatch);
        }
        // Duplicate event IDs cannot overwrite the first event, so equivocation gains nothing.
        // Admission, rather than reduction, retains the evidence.
        if self.applied_event_ids.contains(&event.event_id) {
            return Ok(AdminDocumentApplyStatus::Duplicate);
        }
        let stale_on_all_paths = !matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
                | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
                | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
                | AdminDocumentOperation::RealmConfigTokenRevoked { .. }
                | AdminDocumentOperation::RealmConfigCandidateMapPublished { .. }
                | AdminDocumentOperation::RealmConfigActivationsInitialized { .. }
                | AdminDocumentOperation::RealmConfigTransitionStarted { .. }
                | AdminDocumentOperation::RealmConfigTransitionBarrierReported { .. }
                | AdminDocumentOperation::RealmConfigTransitionProofSubmitted { .. }
                | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
                | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. }
                | AdminDocumentOperation::RealmConfigTransitionStallReported { .. }
                | AdminDocumentOperation::RealmConfigTransitionDrainReported { .. }
        ) && operation_paths(&event.op)
            .iter()
            .all(|path| self.event_path_stale(event, path));
        let apply_status = self.apply_event(event)?;

        if stale_on_all_paths {
            self.applied_event_ids.insert(event.event_id);
            self.clock.advance(event.origin_node_id, event.origin_seq);
            return Ok(AdminDocumentApplyStatus::StaleOriginSequence);
        }

        self.clock.advance(event.origin_node_id, event.origin_seq);
        if apply_status != AdminDocumentApplyStatus::Redundant {
            self.applied_event_ids.insert(event.event_id);
        }
        Ok(apply_status)
    }

    fn apply_event(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        match &event.target {
            AdminDocumentTarget::Group { group_id } => self.apply_group(event, group_id),
            AdminDocumentTarget::Realm { .. } => self.apply_realm(event),
            AdminDocumentTarget::User { .. } => self.apply_user(event),
            AdminDocumentTarget::RealmConfig { realm_id } => self.apply_config(event, realm_id),
        }
    }
}
