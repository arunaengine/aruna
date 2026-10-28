//! Stores, fetches and serves the input pages of one submission. Pages live outside the
//! family records, so they count toward no family read bound and are never lifecycle evidence.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::INPUT_PAGE_KEYSPACE;
use aruna_core::scheduling::PlannedInput;
use aruna_core::structs::execution::job::{
    CapturedInput, InputPage, InputSelection, JobFamilyRecord, LaunchIntent, LogicalJobSpec,
    PageEntries, PageError, PageManifest, SubmissionId, launch_entries, needs_pages, paginate,
    spec_entries,
};
use aruna_core::types::Key;
use thiserror::Error;
use tracing::debug;

use crate::driver::DriverContext;
use crate::forward::authorize::is_sync_eligible;
use crate::jobs::records::keys::submission_prefix;
use crate::metadata::api::load_realm_config;
use crate::placement::resolve_shard_holders;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PageReadError {
    #[error("input page {index} is not available from any holder")]
    Unavailable { index: u32 },
    #[error(transparent)]
    Page(#[from] PageError),
    #[error("input page storage failed: {0}")]
    Storage(String),
}

fn page_key(submission: &SubmissionId, digest: &[u8; 32]) -> Key {
    let mut key = submission_prefix(*submission).to_vec();
    key.extend_from_slice(digest);
    Key::from(key.as_slice())
}

/// Stores pages under their digests. A page is immutable, so a rewrite is a no-op.
pub async fn store_pages(
    context: &DriverContext,
    submission: &SubmissionId,
    pages: &[InputPage],
) -> Result<(), PageReadError> {
    let mut writes = Vec::with_capacity(pages.len());
    for page in pages {
        let digest = page.digest().map_err(PageError::from)?;
        let value = postcard::to_allocvec(page)
            .map_err(|error| PageReadError::Storage(error.to_string()))?;
        writes.push((
            INPUT_PAGE_KEYSPACE.to_string(),
            page_key(submission, &digest),
            value.into(),
        ));
    }
    let effect = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        other => Err(PageReadError::Storage(format!("{other:?}"))),
    }
}

/// The page this node keeps under `digest`, checked against that digest.
pub async fn local_page(
    context: &DriverContext,
    submission: &SubmissionId,
    digest: &[u8; 32],
) -> Result<Option<InputPage>, PageReadError> {
    let effect = StorageEffect::Read {
        key_space: INPUT_PAGE_KEYSPACE.to_string(),
        key: page_key(submission, digest),
        txn_id: None,
    };
    let bytes = match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => return Err(PageReadError::Storage(format!("{other:?}"))),
    };
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let page: InputPage =
        postcard::from_bytes(&bytes).map_err(|error| PageReadError::Storage(error.to_string()))?;
    match page.digest().map_err(PageError::from)? == *digest {
        true => Ok(Some(page)),
        false => Err(PageError::Digest { index: page.index }.into()),
    }
}

/// Every page of `manifest`, read here or fetched from `peers` in order and kept here.
/// Each page must hash to the digest its predecessor or the manifest names.
pub async fn load_chain(
    context: &DriverContext,
    submission: &SubmissionId,
    manifest: &PageManifest,
    peers: &[NodeId],
) -> Result<Vec<InputPage>, PageReadError> {
    let mut pages = Vec::with_capacity(manifest.pages as usize);
    let mut next = (manifest.pages > 0).then_some(manifest.first);
    while let Some(digest) = next {
        let index = pages.len() as u32;
        if index >= manifest.pages {
            return Err(PageError::Chain.into());
        }
        let page = match local_page(context, submission, &digest).await? {
            Some(page) => page,
            None => {
                let page = fetch_page(context, submission, &digest, peers)
                    .await
                    .ok_or(PageReadError::Unavailable { index })?;
                store_pages(context, submission, std::slice::from_ref(&page)).await?;
                page
            }
        };
        next = page.next;
        pages.push(page);
    }
    aruna_core::structs::execution::job::verify_chain(manifest, &pages)?;
    Ok(pages)
}

async fn fetch_page(
    context: &DriverContext,
    submission: &SubmissionId,
    digest: &[u8; 32],
    peers: &[NodeId],
) -> Option<InputPage> {
    let metadata = context.metadata_handle.as_ref()?;
    for peer in peers {
        match metadata
            .request_input_page(*peer, *submission, *digest)
            .await
        {
            Ok(Some(page)) if page.digest().ok().as_ref() == Some(digest) => return Some(page),
            Ok(_) => continue,
            Err(error) => debug!(peer = %peer, error = %error, "Input page fetch failed"),
        }
    }
    None
}

/// The other holders of the submission's family, which keep its pages with its records.
async fn family_holders(context: &DriverContext, submission: SubmissionId) -> Vec<NodeId> {
    let Some(net) = context.net_handle.as_ref() else {
        return Vec::new();
    };
    let Some(config) = load_realm_config(context, *net.realm_id()).await else {
        return Vec::new();
    };
    let holders = (config.family_placement(submission))
        .map(|placement| resolve_shard_holders(&config, &placement))
        .unwrap_or_default();
    holders
        .into_iter()
        .filter(|holder| *holder != net.node_id())
        .collect()
}

/// Keeps the pages a record binds, fetched from the peer that sent it, the nodes it names or
/// the family holders. A holder acknowledges such a record only once this succeeds.
pub async fn keep_pages(
    context: &DriverContext,
    record: &JobFamilyRecord,
    peer: NodeId,
) -> Result<(), PageReadError> {
    let (submission, manifest, mut peers) = match record {
        JobFamilyRecord::Spec(spec) => (
            spec.submission_id,
            spec.input_pages,
            vec![
                peer,
                spec.admission.admitting_node_id,
                spec.ingress_node_id,
                spec.origin_node_id,
            ],
        ),
        JobFamilyRecord::Launch(launch) => (
            launch.submission_id,
            launch.input_pages,
            vec![peer, launch.scheduler_node_id],
        ),
        _ => return Ok(()),
    };
    let Some(manifest) = manifest else {
        return Ok(());
    };
    peers.extend(family_holders(context, submission).await);
    load_chain(context, &submission, &manifest, &peers).await?;
    Ok(())
}

/// The spec with its paged inputs filled in, read here, from the nodes that admitted it or
/// from the family holders.
pub async fn with_inputs(
    context: &DriverContext,
    spec: &LogicalJobSpec,
) -> Result<LogicalJobSpec, PageReadError> {
    let Some(manifest) = spec.input_pages else {
        return Ok(spec.clone());
    };
    let mut peers = vec![
        spec.admission.admitting_node_id,
        spec.ingress_node_id,
        spec.origin_node_id,
    ];
    peers.extend(family_holders(context, spec.submission_id).await);
    let pages = load_chain(context, &spec.submission_id, &manifest, &peers).await?;
    let (inputs, captured): (Vec<_>, Vec<_>) = spec_entries(pages)?.into_iter().unzip();
    let mut filled = spec.clone();
    filled.payload.inputs = inputs;
    filled.captured_inputs = captured;
    Ok(filled)
}

/// The launch's planned inputs, inline or read from its scheduler or the family holders.
pub async fn planned_inputs(
    context: &DriverContext,
    launch: &LaunchIntent,
) -> Result<Vec<PlannedInput>, PageReadError> {
    let Some(manifest) = launch.input_pages else {
        return Ok(launch.inputs.clone());
    };
    let mut peers = vec![launch.scheduler_node_id];
    peers.extend(family_holders(context, launch.submission_id).await);
    let pages = load_chain(context, &launch.submission_id, &manifest, &peers).await?;
    Ok(launch_entries(pages)?)
}

/// Moves a request's inputs into pages kept here when they would not fit its record.
pub async fn page_request(
    context: &DriverContext,
    submission: &SubmissionId,
    inputs: &mut Vec<InputSelection>,
    captured: &mut Vec<CapturedInput>,
) -> Result<Option<PageManifest>, PageReadError> {
    let mut pairs = Vec::with_capacity(inputs.len());
    for input in inputs.iter() {
        let capture = captured
            .iter()
            .find(|capture| capture.destination_key == input.dest_key)
            .ok_or(PageError::Chain)?;
        pairs.push((input.clone(), capture.clone()));
    }
    if !needs_pages(&pairs).map_err(PageError::from)? {
        return Ok(None);
    }
    let (manifest, pages) = paginate(&pairs, PageEntries::Spec).map_err(PageError::from)?;
    store_pages(context, submission, &pages).await?;
    inputs.clear();
    captured.clear();
    Ok(Some(manifest))
}

/// Moves a launch's planned inputs into pages kept here when they would not fit its record.
pub async fn page_launch(
    context: &DriverContext,
    launch: &mut LaunchIntent,
) -> Result<(), PageReadError> {
    if !needs_pages(&launch.inputs).map_err(PageError::from)? {
        return Ok(());
    }
    let (manifest, pages) =
        paginate(&launch.inputs, PageEntries::Launch).map_err(PageError::from)?;
    store_pages(context, &launch.submission_id, &pages).await?;
    launch.inputs.clear();
    launch.input_pages = Some(manifest);
    Ok(())
}

/// Serves one page to a realm infrastructure node; a device never reads another's inputs.
pub async fn serve_page(
    context: &DriverContext,
    peer: NodeId,
    submission: SubmissionId,
    digest: [u8; 32],
) -> Option<Box<InputPage>> {
    let realm_id = context.net_handle.as_ref().map(|net| *net.realm_id())?;
    let config = load_realm_config(context, realm_id).await?;
    if !is_sync_eligible(&config, peer) {
        return None;
    }
    local_page(context, &submission, &digest)
        .await
        .ok()
        .flatten()
        .map(Box::new)
}

#[cfg(test)]
mod tests {
    use aruna_core::structs::execution::job::InputContent;
    use ulid::Ulid;

    use super::*;
    use crate::tests::s3::{test_context, test_storage};

    fn submission() -> SubmissionId {
        SubmissionId([3; 32])
    }

    fn pins() -> Vec<PlannedInput> {
        (0..40u32)
            .map(|index| PlannedInput {
                destination_key: format!("{index:08}{}", "x".repeat(40 * 1024)),
                version_id: Ulid::from_parts(u64::from(index), 1),
                content: InputContent::Blake3([index as u8; 32]),
                bytes: 1,
                policies: Vec::new(),
                source_node_id: None,
                transfer_ms: 0,
                known_link: false,
            })
            .collect()
    }

    #[tokio::test]
    async fn pages_large_requests() {
        // Inputs past the inline budget leave the request for pages, and the stored spec gets
        // them back in order; a small request stays inline.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let family = crate::tests::records::Family::new([6; 32]);
        let mut spec = family.spec();
        let mut inputs = Vec::new();
        let mut captured = Vec::new();
        for index in 0..400u32 {
            let key = format!("db/{index:05}-{}", "x".repeat(800));
            inputs.push(InputSelection {
                source: aruna_core::structs::execution::job::InputSource::S3 {
                    bucket: "bakta".to_string(),
                    key: key.clone(),
                    version_id: Some(Ulid::from_parts(u64::from(index), 1).to_string()),
                },
                source_node_id: None,
                dest_key: key.clone(),
                mode: aruna_core::structs::execution::job::InputMode::Mount,
                container_path: Some(format!("/{key}")),
                name: None,
                description: None,
            });
            captured.push(CapturedInput {
                destination_key: key,
                source_node_id: family.holder.public(),
                version_id: Ulid::from_parts(u64::from(index), 1),
                content: InputContent::Blake3([1; 32]),
                bytes: 1,
                policies: Vec::new(),
                source_group_id: Ulid::from_parts(2, 2),
                public: false,
                link: None,
                source_pin: None,
                database: None,
                releases: Vec::new(),
                releases_complete: false,
                grant: None,
            });
        }
        let (full_inputs, full_captured) = (inputs.clone(), captured.clone());
        let manifest = page_request(&context, &spec.submission_id, &mut inputs, &mut captured)
            .await
            .unwrap()
            .expect("paged");
        assert!(inputs.is_empty() && captured.is_empty() && manifest.pages > 1);
        spec.input_pages = Some(manifest);
        let filled = with_inputs(&context, &spec).await.unwrap();
        assert_eq!(filled.payload.inputs, full_inputs);
        assert_eq!(filled.captured_inputs, full_captured);
        let (mut small, mut small_captured) =
            (full_inputs[..2].to_vec(), full_captured[..2].to_vec());
        let inline = page_request(
            &context,
            &spec.submission_id,
            &mut small,
            &mut small_captured,
        );
        assert_eq!(inline.await.unwrap(), None);
        assert_eq!(small.len(), 2);
    }

    #[tokio::test]
    async fn loads_local_chain() {
        // Stored pages load back in order; a page missing everywhere is unavailable.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let (manifest, pages) = paginate(&pins(), PageEntries::Launch).unwrap();
        store_pages(&context, &submission(), &pages[1..])
            .await
            .unwrap();
        let missing = load_chain(&context, &submission(), &manifest, &[]).await;
        assert_eq!(missing, Err(PageReadError::Unavailable { index: 0 }));
        store_pages(&context, &submission(), &pages[..1])
            .await
            .unwrap();
        let loaded = load_chain(&context, &submission(), &manifest, &[])
            .await
            .unwrap();
        assert_eq!(loaded, pages);
        let other = SubmissionId([5; 32]);
        let foreign = load_chain(&context, &other, &manifest, &[]).await;
        assert_eq!(foreign, Err(PageReadError::Unavailable { index: 0 }));
    }
}
