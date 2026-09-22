//! Writes recoverable native object references for repository files without copying their bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::invenio::{InvenioMode, validate_id};
use aruna_core::structs::execution::job::{ImportRoCrateSource, ImportRoCrateSpec};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::BucketInfo;
use serde_json::Value;
use ulid::Ulid;

use super::{TransferError, connect, interruptible};
use crate::connectors::resolver::{ResolveConnectorInput, ResolveConnectorOperation};
use crate::driver::drive;
use crate::jobs::executor::JobContext;
use crate::staging::descriptor::build_source_binding;
use crate::staging::reference::{ReferenceWrite, write_reference_version};

pub(crate) fn is_reference(spec: &ImportRoCrateSpec, path: &str) -> bool {
    matches!(&spec.source, ImportRoCrateSource::Invenio { options, .. } if options.mode == InvenioMode::Reference)
        && path.starts_with("versions/")
        && path.contains("/files/")
}

pub(crate) async fn write_reference(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    bucket: BucketInfo,
    key: &str,
    version_id: Ulid,
    descriptor: &Value,
) -> Result<SourceMetadata, TransferError> {
    let ImportRoCrateSource::Invenio {
        group_id,
        connector_id,
        ..
    } = &spec.source
    else {
        return Err(TransferError::Permanent(
            "reference requires repository source".into(),
        ));
    };
    if bucket.group_id != *group_id {
        return Err(TransferError::Permanent(
            "reference target and connector must share a group".into(),
        ));
    }
    let resolved = interruptible(ctx, async {
        drive(
            ResolveConnectorOperation::new(ResolveConnectorInput {
                group_id: *group_id,
                connector_id: *connector_id,
                source_path: String::new(),
                allow_root: true,
            }),
            &ctx.driver,
        )
        .await
        .map_err(|error| match error {
            aruna_core::errors::SourceResolutionError::StorageError(_) => {
                TransferError::Retryable("repository connector storage unavailable".into())
            }
            _ => invalid("repository connector unavailable"),
        })
    })
    .await?;
    let client = interruptible(
        ctx,
        connect(
            &ctx.driver,
            &spec.auth_context,
            *group_id,
            *connector_id,
            Permission::READ,
            spec.limits.metadata_bytes,
            None,
        ),
    )
    .await?;
    let id = descriptor["record_id"]
        .as_str()
        .ok_or_else(|| invalid("missing reference record"))?;
    validate_id(id)?;
    let file = &descriptor["file"];
    let name = file["key"]
        .as_str()
        .ok_or_else(|| invalid("missing reference key"))?;
    let url = client.url(&["records", id, "files", name, "content"])?;
    let metadata = interruptible(ctx, async { Ok(client.head(url).await?) }).await?;
    if file["size"].as_u64() != Some(metadata.content_length) {
        return Err(invalid("repository reference size changed"));
    }
    let mut connector = resolved.connector.clone();
    if connector
        .public_config
        .get("endpoint")
        .map(|endpoint| format!("{}/", endpoint.trim_end_matches('/')))
        .as_deref()
        != Some(client.endpoint())
    {
        return Err(invalid("repository connector changed"));
    }
    // Keep the encoded file key in the endpoint; OpenDAL only encodes the final path component.
    connector.public_config.insert(
        "endpoint".into(),
        client.url(&["records", id, "files", name])?.to_string(),
    );
    let source = build_source_binding(
        StagingStrategy::Reference,
        &connector,
        &metadata,
        "content".into(),
        Some(ctx.owner_node_id),
        Some(*connector_id),
    );
    if ctx.cancel.is_cancelled() {
        return Err(TransferError::Cancelled);
    }
    if ctx.shutdown.is_cancelled() {
        return Err(TransferError::Interrupted);
    }
    write_reference_version(
        &ctx.driver,
        ReferenceWrite {
            preassigned_version_id: Some(version_id),
            group_id: *group_id,
            user_id: spec.auth_context.user_id,
            realm_id: spec.auth_context.realm_id,
            node_id: ctx.owner_node_id,
            bucket: spec.target.bucket.clone(),
            key: key.into(),
            expected_bucket: Some(bucket),
            version_source: source,
            metadata: metadata.clone(),
            inherited_policies: Vec::new(),
            connector_guard: Some((resolved.connector, resolved.secret_fingerprint)),
        },
    )
    .await
    .map_err(|error| TransferError::Retryable(error.to_string()))?;
    Ok(metadata)
}

fn invalid(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}
