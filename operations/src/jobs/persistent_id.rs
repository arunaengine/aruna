//! Runs the job that mints a w3id persistent identifier for a metadata document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{JobError, JobResultPayload};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use aruna_core::structs::{MintPersistentSpec, PersistentIdFailure};
use aruna_core::time::unix_timestamp_millis;

use crate::driver::{DriverContext, drive};
use crate::metadata::api::MetadataApiError;
use crate::metadata::persistent_id::forward::{PidNode, PidRouteOperation, outcome_mapping};
use crate::metadata::protocol::PersistentIdRequest;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::realm::RealmId;

use crate::jobs::executor::{JobContext, JobRunOutcome};

/// Register a w3id PID for a document. The mint is a compare-and-set on the
/// document's authority, so a lost race or post-withdrawal run reports the
/// authoritative mapping with `newly_minted: false`; routing never mints locally.
pub async fn run_mint_pid(ctx: &JobContext, spec: &MintPersistentSpec) -> JobRunOutcome {
    let realm_id = ctx.driver.serving_realm(spec.minted_by.realm_id);
    // The submitting route requires an unrestricted realm token, so the internal
    // principal the authority re-checks carries no path restrictions to drop.
    let auth_token = AuthToken::internal(AuthContext {
        user_id: spec.minted_by,
        realm_id,
        path_restrictions: None,
        session: None,
    });
    let request = PersistentIdRequest::Mint {
        minted_by: spec.minted_by,
        minted_at_ms: unix_timestamp_millis(),
    };
    let caller = (Some(auth_token.clone()), None);
    let mint = PidRouteOperation::new(
        pid_node(&ctx.driver, realm_id),
        spec.document_id,
        request,
        caller,
    );
    match drive(mint, &ctx.driver).await.and_then(outcome_mapping) {
        Ok((mapping, newly_minted)) if mapping.is_active() || mapping.is_retired() => {
            JobRunOutcome::Succeeded(JobResultPayload::PersistentId {
                pid: mapping.pid,
                newly_minted,
            })
        }
        Ok((_, _)) | Err(MetadataApiError::NotFound) => JobRunOutcome::Deferred(
            JobError::retryable("persistent id mint is waiting for metadata projection"),
        ),
        Err(error @ (MetadataApiError::Unauthorized | MetadataApiError::Forbidden)) => {
            record_failure(ctx, spec, &auth_token, error.to_string(), false).await;
            JobRunOutcome::Failed(JobError::permanent(format!("persistent id mint: {error}")))
        }
        Err(error) => {
            if ctx.final_attempt {
                record_failure(ctx, spec, &auth_token, error.to_string(), true).await;
            }
            JobRunOutcome::Failed(JobError::retryable(format!("persistent id mint: {error}")))
        }
    }
}

/// How long identifier registration waits for a missing mapping without using attempts.
const IDENTIFIER_WAIT_MS: u64 = 600_000;

/// Adds repository identifiers through the document's authority. An unreachable authority
/// defers the job without using an attempt, so registration waits for it instead of failing.
pub async fn run_register_identifiers(
    ctx: &JobContext,
    spec: &RegisterIdentifiersSpec,
) -> JobRunOutcome {
    let request = PersistentIdRequest::AddIdentifiers {
        identifiers: spec.identifiers.clone(),
        occurred_at_ms: unix_timestamp_millis(),
    };
    let node = pid_node(&ctx.driver, spec.auth_context.realm_id);
    let caller = (None, Some(spec.auth_context.clone()));
    let add = PidRouteOperation::new(node, spec.document_id, request, caller);
    let result = drive(add, &ctx.driver).await.and_then(outcome_mapping);
    match result {
        Ok((_, changed)) => JobRunOutcome::Succeeded(JobResultPayload::Identifiers { changed }),
        // The authority may not have the mapping yet; after the wait, attempts bound the retries.
        Err(MetadataApiError::NotFound)
            if unix_timestamp_millis().saturating_sub(ctx.job_id.timestamp_ms())
                < IDENTIFIER_WAIT_MS =>
        {
            JobRunOutcome::Deferred(JobError::retryable(
                "registering identifiers is waiting for the persistent id mapping",
            ))
        }
        Err(error @ MetadataApiError::NotFound) => JobRunOutcome::Failed(JobError::retryable(
            format!("registering identifiers: {error}"),
        )),
        Err(error @ (MetadataApiError::Forbidden | MetadataApiError::Unauthorized)) => {
            tracing::warn!(document_id = %spec.document_id, %error, "identifiers not registered");
            JobRunOutcome::Failed(JobError::permanent(format!(
                "registering identifiers: {error}"
            )))
        }
        Err(error) => JobRunOutcome::Deferred(JobError::retryable(format!(
            "registering identifiers: {error}"
        ))),
    }
}

async fn record_failure(
    ctx: &JobContext,
    spec: &MintPersistentSpec,
    auth_token: &AuthToken,
    message: String,
    retryable: bool,
) {
    let failure = PersistentIdFailure {
        message,
        retryable,
        recorded_at_ms: unix_timestamp_millis(),
    };
    let realm_id = ctx.driver.serving_realm(spec.minted_by.realm_id);
    let request = PersistentIdRequest::Fail { failure };
    let caller = (Some(auth_token.clone()), None);
    let fail = PidRouteOperation::new(
        pid_node(&ctx.driver, realm_id),
        spec.document_id,
        request,
        caller,
    );
    let _ = drive(fail, &ctx.driver).await;
}

/// The PID route of `realm_id` from this node.
fn pid_node(context: &DriverContext, realm_id: RealmId) -> PidNode {
    (
        realm_id,
        context.node_and_realm().map(|(node_id, _)| node_id),
    )
}
