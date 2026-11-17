//! Ignored load test that documents written at a steady pace under load converge on every node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use aruna_operations::driver::DriverContext;
use ulid::Ulid;

use crate::convergence::{wait_empty_queues, wait_for_visibility};
use crate::topology::{
    BoxError, build_realm_nodes, init_logging, make_runtime, mixed_payload, node_targets,
    WriterSettings, run_writers, shutdown_nodes,
};

const WRITERS: usize = 24;
const DOCS_PER_WRITER: usize = 500;
const WRITER_PERIOD: Duration = Duration::from_millis(40);

// The load of the propagation tail seen in the 3-node cluster. Its latency
// percentiles are measured by the measure example.
#[test]
#[ignore]
fn sustained_load_converges() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    runtime.block_on(async {
        let realm_id = RealmId([125u8; 32]);
        let nodes = build_realm_nodes(&realm_id, 3).await?;
        let targets = node_targets(&nodes);
        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        let group_id = Ulid::generate();

        let created = run_writers(
            &WriterSettings {
                realm_id,
                group_id,
                label: "tail".to_string(),
                period: Some(WRITER_PERIOD),
                payload: mixed_payload,
            },
            WRITERS,
            DOCS_PER_WRITER,
            &targets,
        )
        .await?;
        let pairs: Vec<(GroupId, Ulid)> = created
            .iter()
            .map(|(group_id, document_id, _)| (*group_id, *document_id))
            .collect();
        let result = match wait_for_visibility(&contexts, &pairs, Duration::from_millis(200)).await
        {
            Ok(()) => wait_empty_queues(&contexts).await,
            Err(error) => Err(error),
        };
        shutdown_nodes(nodes).await;
        Ok::<(), BoxError>(result?)
    })?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    Ok(())
}
