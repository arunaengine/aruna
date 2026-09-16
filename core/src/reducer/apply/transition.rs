use super::*;

impl AdminDocumentState {
    pub(super) fn apply_transition(
        &mut self,
        event: &AdminDocumentEvent,
        realm_id: &RealmId,
    ) -> Result<AdminApplyStatus, AdminDocumentError> {
        match &event.op {
            AdminDocumentOperation::ConfigTransitionStarted { plan } => {
                let mut seen = BTreeSet::new();
                let well_formed = plan.limits.max_incomplete_buckets >= 1
                    && plan.target_map_epoch > 0
                    && !plan.buckets.is_empty()
                    && plan.buckets.iter().all(|bucket| {
                        seen.insert(bucket.bucket) && !bucket.target_holders.is_empty()
                    });
                if !well_formed {
                    return Err(AdminDocumentError::InvalidTransitionPlan);
                }
                self.apply_immutable_value(
                    event,
                    transition_path(&plan.transition_id),
                    transition_plan_value(plan),
                );
            }
            AdminDocumentOperation::TransitionBarrierReported {
                transition_id,
                bucket,
                reported_by,
                frontier,
            } => {
                if *reported_by != event.origin_node_id {
                    return Err(AdminDocumentError::TransitionOriginMismatch);
                }
                if frontier.len() > crate::structs::placement::transition::MAX_FRONTIER_BYTES {
                    return Err(AdminDocumentError::TransitionReportOversized);
                }
                self.apply_transition_report(
                    event,
                    transition_barrier_path(transition_id, *bucket, reported_by),
                    hex::encode(frontier),
                );
            }
            AdminDocumentOperation::TransitionProofSubmitted {
                transition_id,
                strategy_id,
                proof,
            } => {
                if proof.holder != event.origin_node_id {
                    return Err(AdminDocumentError::TransitionOriginMismatch);
                }
                // A replicated plan wins; otherwise materialization rechecks the signed submitted
                // strategy against the plan.
                let strategy_id = self
                    .materialized_transition_plans()
                    .get(transition_id)
                    .map(|plan| plan.strategy_id)
                    .unwrap_or(*strategy_id);
                if !proof.verify(*realm_id, *transition_id, strategy_id) {
                    return Err(AdminDocumentError::InvalidTransitionProof);
                }
                self.apply_transition_report(
                    event,
                    transition_proof_path(transition_id, proof.bucket, &proof.holder),
                    transition_proof_value(&strategy_id, proof),
                );
            }
            AdminDocumentOperation::ConfigTransitionAborted { transition_id } => {
                self.apply_immutable_value(
                    event,
                    transition_abort_path(transition_id),
                    true.to_string(),
                );
            }
            AdminDocumentOperation::TransitionBucketForced {
                transition_id,
                bucket,
                at_risk_report,
            } => {
                if at_risk_report.len() > crate::structs::placement::transition::MAX_STALL_BYTES {
                    return Err(AdminDocumentError::TransitionReportOversized);
                }
                self.apply_transition_report(
                    event,
                    transition_force_path(transition_id, *bucket),
                    at_risk_report.clone(),
                );
            }
            AdminDocumentOperation::TransitionStallReported {
                transition_id,
                bucket,
                reported_by,
                reason,
            } => {
                if *reported_by != event.origin_node_id {
                    return Err(AdminDocumentError::TransitionOriginMismatch);
                }
                if reason.len() > crate::structs::placement::transition::MAX_STALL_BYTES {
                    return Err(AdminDocumentError::TransitionReportOversized);
                }
                self.apply_transition_report(
                    event,
                    transition_stall_path(transition_id, *bucket, reported_by),
                    reason.clone(),
                );
            }
            AdminDocumentOperation::TransitionDrainReported {
                transition_id,
                bucket,
                reported_by,
            } => {
                if *reported_by != event.origin_node_id {
                    return Err(AdminDocumentError::TransitionOriginMismatch);
                }
                self.apply_transition_report(
                    event,
                    transition_drain_path(transition_id, *bucket, reported_by),
                    true.to_string(),
                );
            }
            AdminDocumentOperation::HandleRangeGranted { range } => {
                if !range.is_well_formed() {
                    return Err(AdminDocumentError::InvalidHandleRange);
                }
                self.apply_handle_range(event, range);
            }
            AdminDocumentOperation::BandPoolAssigned { pool } => {
                if !pool.is_well_formed() {
                    return Err(AdminDocumentError::InvalidHandleRange);
                }
                self.apply_band_pool(event, pool);
            }
            _ => return Err(AdminDocumentError::UnsupportedTarget),
        }
        Ok(AdminApplyStatus::Applied)
    }
}
