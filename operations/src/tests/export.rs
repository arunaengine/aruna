//! Builds the specs, documents, job contexts and probes the export tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::driver::drive;
use crate::jobs::context::JobContext;
use crate::jobs::executor::ProgressReporter;
use crate::jobs::export::archive::recognize_entities;
use crate::jobs::export::probe::{CandidateFailures, ProbeSourcesOperation};
use crate::jobs::export::sources::AliasState;
use crate::jobs::export::*;
use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::execution::job::{ExportRoCrateSpec, JobId};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::sync::Arc;
use ulid::Ulid;

pub fn remote_spec(realm_id: RealmId, user_id: UserId) -> ExportRoCrateSpec {
    ExportRoCrateSpec {
        selection: None,
        destination: None,
        auth_context: AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        document_id: Ulid::from_bytes([91; 16]),
        limits: RoCrateLimits::default(),
    }
}

pub fn recognized_entities(
    document: &JsonValue,
    realm_id: RealmId,
) -> Result<Vec<ExportEntity>, ExportFailure> {
    let jsonld = serde_json::to_string(document).unwrap();
    let canonical = craqle::canonicalize_jsonld(&jsonld).unwrap();
    recognize_entities(document, &canonical.nquads, realm_id)
}

pub fn job_context(driver: Arc<DriverContext>, owner_node_id: NodeId) -> JobContext {
    JobContext {
        driver,
        job_id: JobId::from_bytes([84; 16]),
        owner_node_id,
        claim_token: Ulid::from_bytes([85; 16]),
        final_attempt: false,
        cancel: tokio_util::sync::CancellationToken::new(),
        shutdown: tokio_util::sync::CancellationToken::new(),
        progress: ProgressReporter::from_progress(
            &aruna_core::structs::execution::job::JobProgress {
                current: 0,
                total: None,
                unit: "entries".to_string(),
                paused_until: None,
            },
        ),
        range_turns: Default::default(),
        handlers: Default::default(),
    }
}

pub fn crate_document(parts: &[JsonValue]) -> JsonValue {
    let mut graph = vec![
        json!({
            "@id": "./",
            "@type": "Dataset",
            "name": "test",
            "description": "test crate",
            "datePublished": "2026-07-23",
            "hasPart": parts
                .iter()
                .map(|part| json!({"@id": part["@id"]}))
                .collect::<Vec<_>>()
        }),
        json!({
            "@id": METADATA_PATH,
            "@type": "CreativeWork",
            "about": {"@id": "./"},
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}
        }),
    ];
    graph.extend(parts.iter().cloned());
    json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph})
}

pub fn scope_of(ctx: &JobContext, spec: &ExportRoCrateSpec) -> SourceScope {
    SourceScope {
        spec: Arc::new(spec.clone()),
        owner_node: ctx.owner_node_id,
    }
}

pub async fn probe_sources(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    failures: &CandidateFailures,
) -> Result<Vec<ProbedEntry>, ExportFailure> {
    let probe = ProbeSourcesOperation::new(
        scope_of(ctx, spec),
        checkpoint.entities.clone(),
        failures.clone(),
        AliasState::default(),
    );
    let probed = drive(probe, ctx.driver.as_ref()).await?;
    checkpoint.entities = probed.entities;
    Ok(probed.entries)
}

pub fn destination() -> aruna_core::repository::RepositoryDestination {
    aruna_core::repository::RepositoryDestination {
        group_id: Ulid::from_parts(2, 2),
        connector_id: Ulid::from_parts(4, 4),
        draft_id: None,
        published_id: None,
        metadata_json: "{}".to_string(),
        publish: false,
        public_files: false,
        credential: None,
        link: None,
    }
}
