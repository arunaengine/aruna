//! Publishes one finished local run: a realm node pulls each output, then its crate is written.
//! Outputs move by exact version through the synced-folder pull, so a retried publish replays.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::execution::job::{
    JobId, JobPayload, JobResultPayload, OutputObject, RunCrateStatus,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::data_identity::parse_hash;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::{RemoteBinding, SyncRefusal};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::JobRunOutcome;
use crate::jobs::store::{read_crate_status, read_job_record};
use crate::jobs::workflow::run_crate::record_crate;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::get_config::GetConfigOperation;

/// One publish at a time, so a repeated request cannot mint a second crate.
static PUBLISHING: Mutex<()> = Mutex::const_new(());

pub struct PublishRunInput {
    pub job_id: JobId,
    /// The user this device is enrolled for; only their runs are published.
    pub owner: UserId,
    /// This device, which ran the job and serves its outputs.
    pub node_id: NodeId,
    /// Realm node, bucket and prefix the outputs are copied into.
    pub target: RemoteBinding,
}

#[derive(Debug, Error)]
pub enum PublishRunError {
    #[error("no such local run")]
    NotFound,
    #[error("the run has not finished")]
    NotFinished,
    #[error("node {0} is not a realm server")]
    NotRealmNode(NodeId),
    #[error("the realm refused output {key}: {refusal:?}")]
    Refused { key: String, refusal: SyncRefusal },
    #[error("the realm node is unreachable: {0}")]
    Unreachable(String),
    #[error("two outputs of the run would land on {0}")]
    Collision(String),
    #[error("the run crate was not written: {0}")]
    Crate(String),
    #[error("device state unavailable: {0}")]
    Unavailable(String),
}

/// The realm copies of the run's outputs and the crate outcome.
#[derive(Debug)]
pub struct PublishedRun {
    pub outputs: Vec<OutputObject>,
    pub run_crate: RunCrateStatus,
}

/// Copies a finished local run's outputs into a realm bucket, then writes its crate there.
///
/// Every output is pulled before the crate is written, so the crate names the realm copies.
pub async fn publish_run(
    context: &Arc<DriverContext>,
    input: PublishRunInput,
) -> Result<PublishedRun, PublishRunError> {
    let _publishing = PUBLISHING.lock().await;
    let storage = &context.storage_handle;
    let record = read_job_record(storage, input.job_id, None)
        .await
        .map_err(PublishRunError::Unavailable)?
        .ok_or(PublishRunError::NotFound)?;
    let JobPayload::Execution(spec) = &record.payload else {
        return Err(PublishRunError::NotFound);
    };
    if record.created_by != input.owner || record.owner_node_id != input.node_id {
        return Err(PublishRunError::NotFound);
    }
    if !record.state.is_terminal() {
        return Err(PublishRunError::NotFinished);
    }
    let status = read_crate_status(storage, input.job_id)
        .await
        .map_err(PublishRunError::Unavailable)?;
    let saved_id = match &status {
        // A notebook session owes no crate and is no run to publish.
        None => return Err(PublishRunError::NotFound),
        Some(RunCrateStatus::Pending) => return Err(PublishRunError::NotFinished),
        Some(RunCrateStatus::Minted { document_id }) => Some(*document_id),
        Some(_) => None,
    };
    let realm_id = context.serving_realm(input.owner.realm_id);
    let config = drive(GetConfigOperation::new(realm_id), context.as_ref())
        .await
        .map_err(|error| PublishRunError::Unavailable(error.to_string()))?;
    let eligible = config
        .sync_eligible_nodes()
        .map_err(|error| PublishRunError::Unavailable(error.to_string()))?;
    if !eligible.contains(&input.target.node_id) {
        return Err(PublishRunError::NotRealmNode(input.target.node_id));
    }
    let auth = AuthContext {
        user_id: input.owner,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let outputs = match &record.result {
        Some(JobResultPayload::Execution { outputs, .. }) => outputs.clone(),
        _ => Vec::new(),
    };
    let keys = target_keys(&input.target, &outputs)?;
    let mut copies = Vec::with_capacity(outputs.len());
    for (output, key) in outputs.iter().zip(keys) {
        copies.push(pull_output(context, &auth, &input.target, output, key).await?);
    }
    if let Some(written @ RunCrateStatus::Written { .. }) = status {
        return Ok(PublishedRun {
            outputs: copies,
            run_crate: written,
        });
    }
    let mut published = record.clone();
    if let Some(JobResultPayload::Execution { outputs, .. }) = &mut published.result {
        *outputs = copies.clone();
    }
    match record_crate(context, input.job_id, &published, spec, saved_id).await {
        JobRunOutcome::Succeeded(_) => {}
        JobRunOutcome::Failed(error) | JobRunOutcome::Deferred(error) => {
            return Err(PublishRunError::Crate(error.message));
        }
        _ => return Err(PublishRunError::Crate("interrupted".to_string())),
    }
    let run_crate = read_crate_status(storage, input.job_id)
        .await
        .map_err(PublishRunError::Unavailable)?
        .ok_or(PublishRunError::NotFound)?;
    Ok(PublishedRun {
        outputs: copies,
        run_crate,
    })
}

/// The realm key of every output, `<prefix>/<source bucket>/<key>`, so outputs of different
/// buckets never meet. Two outputs on one key are refused before anything is copied.
fn target_keys(
    target: &RemoteBinding,
    outputs: &[OutputObject],
) -> Result<Vec<String>, PublishRunError> {
    let mut seen = HashSet::new();
    outputs
        .iter()
        .map(|output| {
            let key = target.remote_key(&format!("{}/{}", output.bucket, output.key));
            match seen.insert(key.clone()) {
                true => Ok(key),
                false => Err(PublishRunError::Collision(key)),
            }
        })
        .collect()
}

/// Asks the realm node to pull one output version and reports the copy it made.
async fn pull_output(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    target: &RemoteBinding,
    output: &OutputObject,
    key: String,
) -> Result<OutputObject, PublishRunError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| PublishRunError::Unavailable("metadata handle".to_string()))?;
    let source = VersionedObjectArn::new(
        auth.realm_id,
        output.node_id,
        output.bucket.clone(),
        output.key.clone(),
        output.version_id,
    )
    .map_err(|error| PublishRunError::Unavailable(error.to_string()))?;
    let message = MetadataTransportMessage::ForwardSyncPull {
        auth_token: AuthToken::internal(auth.clone()),
        source: Box::new(source),
        blake3: output.digest.as_deref().and_then(parse_hash),
        size: output.size,
        target_bucket: target.bucket.clone(),
        target_key: key.clone(),
        deleted: false,
    };
    match metadata
        .request_forwarded_write(target.node_id, message)
        .await
    {
        Ok(MetadataTransportMessage::ForwardedSyncPull { result: Ok(ack) }) => Ok(OutputObject {
            node_id: target.node_id,
            bucket: target.bucket.clone(),
            key,
            version_id: ack.version_id,
            ..output.clone()
        }),
        Ok(MetadataTransportMessage::ForwardedSyncPull {
            result: Err(refusal),
        }) => Err(PublishRunError::Refused {
            key: output.key.clone(),
            refusal,
        }),
        Ok(other) => Err(PublishRunError::Unreachable(
            crate::metadata::transport_message_kind(&other).to_string(),
        )),
        Err(error) => Err(PublishRunError::Unreachable(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn output(bucket: &str, key: &str) -> OutputObject {
        OutputObject {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Ulid::from(2u128),
            execution_id: Ulid::from(3u128),
            container_path: String::new(),
            size: 1,
            digest: None,
        }
    }

    // One file name in two buckets lands on two keys; the same key twice is refused.
    #[test]
    fn keeps_buckets_apart() {
        let target = RemoteBinding {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            bucket: "lab-data".to_string(),
            prefix: "runs/one".to_string(),
        };
        let keys = target_keys(
            &target,
            &[output("a", "report.txt"), output("b", "report.txt")],
        )
        .expect("distinct buckets map apart");
        assert_eq!(keys, ["runs/one/a/report.txt", "runs/one/b/report.txt"]);
        assert!(matches!(
            target_keys(
                &target,
                &[output("a", "report.txt"), output("a", "report.txt")]
            ),
            Err(PublishRunError::Collision(key)) if key == "runs/one/a/report.txt"
        ));
    }
}
