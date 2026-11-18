//! Starts the RO-Crate import and export jobs with their job host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::job::ImportRoCrateSpec;
use rand::RngExt;

use super::job_host::run_hosted;
use crate::driver::node_routing;
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::import::{ImportJobOperation, ImportRun};
use crate::jobs::repository::{JobScope, NodeScope};

pub async fn run_rocrate_import(ctx: &JobContext, spec: &ImportRoCrateSpec) -> JobRunOutcome {
    let run = ImportRun {
        spec: Arc::new(spec.clone()),
        job_id: ctx.job_id,
        claim_token: ctx.claim_token,
        owner_node: ctx.owner_node_id,
        local_node: ctx.driver.node_and_realm().map(|(node, _)| node),
        routing: node_routing(&ctx.driver),
        scope: NodeScope::of(&ctx.driver),
        job: JobScope::of(ctx),
        seed: rand::rng().random(),
    };
    run_hosted(ctx, ImportJobOperation::new(run)).await
}
