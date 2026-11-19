//! Starts the RO-Crate import and export jobs with their job host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::job::{ExportRoCrateSpec, ImportRoCrateSpec};
use rand::RngExt;

use super::job_host::run_hosted;
use aruna_operations::driver::node_routing;
use aruna_operations::jobs::context::JobContext;
use aruna_operations::jobs::executor::JobRunOutcome;
use aruna_operations::jobs::export::{ExportJobOperation, ExportScope};
use aruna_operations::jobs::import::{ImportJobOperation, ImportRun};

pub async fn run_rocrate_import(ctx: &JobContext, spec: &ImportRoCrateSpec) -> JobRunOutcome {
    let run = ImportRun {
        spec: Arc::new(spec.clone()),
        job_id: ctx.job_id,
        claim_token: ctx.claim_token,
        owner_node: ctx.owner_node_id,
        local_node: ctx.driver.node_and_realm().map(|(node, _)| node),
        routing: node_routing(&ctx.driver),
        scope: ctx.driver.node_scope(),
        job: ctx.job_scope(),
        seed: rand::rng().random(),
    };
    run_hosted(ctx, ImportJobOperation::new(run)).await
}

pub async fn run_export_job(ctx: &JobContext, spec: &ExportRoCrateSpec) -> JobRunOutcome {
    let run = ExportScope {
        spec: Arc::new(spec.clone()),
        job_id: ctx.job_id,
        claim_token: ctx.claim_token,
        owner_node: ctx.owner_node_id,
        local_node: ctx.driver.node_and_realm().map(|(node, _)| node),
        scope: ctx.driver.node_scope(),
        job: ctx.job_scope(),
    };
    run_hosted(ctx, ExportJobOperation::new(run)).await
}
