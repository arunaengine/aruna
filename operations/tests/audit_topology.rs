// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! Distributed audit coverage: rows are node-local; non-holders fan out to
//! metadata nodes, merge pages, and report unreachable nodes as partial rather
//! than a false 200.

mod topology;

use aruna_core::StructuredId;
use aruna_core::structs::PlacementRef;
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::drive;
use aruna_operations::metadata::audit::{
    ListAuditOperation, ListAuditRequest, LocalAuditPageOperation, MAX_AUDIT_PAGE_SIZE, list_audit,
};
use aruna_operations::metadata::projector::replay_metadata_event_log;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;
const DOCUMENTS: usize = 5;
/// Lost-progress bound for the peer fan-out, far under the API request timeout.
const FAN_OUT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

#[tokio::test]
async fn nonholder_audit_trail() -> TestResult<()> {
    // A non-holder assembles the whole trail: multi-holder rows collapse to one,
    // paging crosses the merge, and a dead node shows up as partial.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;

    let origin = realm.node(0);
    let mut documents = Vec::new();
    for index in 0..DOCUMENTS {
        let path = format!("datasets/audit-{index}");
        let document_id =
            mint_local_document(&realm.config, &realm.actor(origin), group_id, &path)?.as_ulid();
        let placement = create_document(&realm, origin, group_id, document_id, &path).await?;
        for holder in realm.assert_holder(origin.node_id(), &placement) {
            let node = realm.find(holder);
            wait_until("audit row reaches holder", node.node_id(), || {
                audit_has_document(node, group_id, document_id)
            })
            .await?;
        }
        documents.push((document_id, placement));
    }

    // A record on every holder of its bucket must appear once, not once per holder.
    let (first_document, first_placement) = documents[0];
    let holders = realm.assert_holder(origin.node_id(), &first_placement);
    assert!(holders.len() >= 2, "fixture must replicate to prove dedup");
    for holder in &holders {
        assert!(
            audit_has_document(realm.find(*holder), group_id, first_document).await,
            "holder {holder} is missing the shared audit row"
        );
    }

    // Read the trail from a node that holds none of the first document's bucket.
    let reader = realm.non_holder(&first_placement);
    let full = list_audit(
        reader.context.as_ref(),
        realm.realm_id,
        reader.node_id(),
        Some(realm.bearer_token()),
        request(group_id, None, None, DOCUMENTS * 4),
    )
    .await?;
    assert!(!full.partial, "every node was reachable");
    assert_eq!(unique_documents(&full.records), DOCUMENTS);

    // Paging crosses the merge boundary without dropping or duplicating a record.
    let mut cursor = None;
    let mut seen = Vec::new();
    let mut pages = 0;
    loop {
        let page = list_audit(
            reader.context.as_ref(),
            realm.realm_id,
            reader.node_id(),
            Some(realm.bearer_token()),
            request(group_id, None, cursor.clone(), 2),
        )
        .await?;
        assert!(page.records.len() <= 2, "page exceeded the requested limit");
        pages += 1;
        seen.extend(page.records.iter().map(|record| record.document_id));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
        assert!(pages < 20, "audit paging never terminated");
    }
    assert!(pages >= 2, "the trail did not cross a page boundary");
    seen.sort();
    let deduped = {
        let mut unique = seen.clone();
        unique.dedup();
        unique.len()
    };
    assert_eq!(seen.len(), DOCUMENTS, "a record was paged more than once");
    assert_eq!(deduped, DOCUMENTS, "paging returned a duplicate record");

    // An unreachable node is reported, never hidden behind a complete-looking page.
    let mut peers: Vec<aruna_core::NodeId> = realm
        .nodes
        .iter()
        .filter(|node| node.is_sync_eligible() && node.node_id() != reader.node_id())
        .map(TestNode::node_id)
        .collect();
    let dead: Vec<aruna_core::NodeId> = (200u8..=204)
        .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
        .collect();
    peers.extend(dead.iter().copied());
    // Unreachable peers are asked in one concurrent round, so the fan-out still
    // answers well inside the request deadline instead of summing their waits.
    let partial = tokio::time::timeout(
        FAN_OUT_DEADLINE,
        drive(
            ListAuditOperation::new(
                group_id,
                None,
                peers,
                None,
                MAX_AUDIT_PAGE_SIZE,
                Some(realm.bearer_token()),
                realm.config.digest()?,
            ),
            reader.context.as_ref(),
        ),
    )
    .await
    .map_err(|_| "the audit fan-out exceeded the request deadline")??;
    assert!(partial.partial, "a dead node must mark the page partial");
    for node in &dead {
        assert!(
            partial.missing_nodes.contains(node),
            "every dead node must be named in missing_nodes"
        );
    }
    assert_eq!(
        unique_documents(&partial.records),
        DOCUMENTS,
        "live holders still supply the full trail"
    );

    // An offline configured holder must surface through list_audit as missing:
    // the completeness set is the configured membership, not who is reachable.
    let offline = holders[0];
    realm.find(offline).net.shutdown().await;
    let offline_audit = list_audit(
        reader.context.as_ref(),
        realm.realm_id,
        reader.node_id(),
        Some(realm.bearer_token()),
        request(group_id, None, None, DOCUMENTS * 4),
    )
    .await?;
    assert!(
        offline_audit.partial,
        "an offline configured holder must mark the audit partial"
    );
    assert!(
        offline_audit.missing_nodes.contains(&offline),
        "the offline configured holder must be named in missing_nodes"
    );

    realm.shutdown().await;
    Ok(())
}

fn request(
    group_id: Ulid,
    document_id: Option<Ulid>,
    cursor: Option<String>,
    limit: usize,
) -> ListAuditRequest {
    ListAuditRequest {
        group_id,
        document_id,
        cursor,
        limit: Some(limit),
    }
}

fn unique_documents(records: &[aruna_core::structs::MetadataAuditRecord]) -> usize {
    let mut ids: Vec<Ulid> = records.iter().map(|record| record.document_id).collect();
    ids.sort();
    ids.dedup();
    ids.len()
}

fn document_config(
    realm: &Topology,
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> CreateMetadataDocumentConfig {
    CreateMetadataDocumentConfig {
        actor: realm.actor(node),
        group_id,
        document_id,
        document_path: document_path.to_string(),
        public: true,
        payload: CreateMetadataDocumentPayload::Scaffold {
            name: "Audit Dataset".to_string(),
            description: "Written to exercise the distributed audit trail".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
        },
    }
}

async fn create_document(
    realm: &Topology,
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> TestResult<PlacementRef> {
    let created = drive(
        CreateMetadataDocumentOperation::new(document_config(
            realm,
            node,
            group_id,
            document_id,
            document_path,
        )),
        node.context.as_ref(),
    )
    .await?;
    replay_metadata_event_log(node.context.as_ref()).await?;
    Ok(created.record.placement)
}

async fn audit_has_document(node: &TestNode, group_id: Ulid, document_id: Ulid) -> bool {
    drive(
        LocalAuditPageOperation::new(group_id, Some(document_id), None, MAX_AUDIT_PAGE_SIZE),
        node.context.as_ref(),
    )
    .await
    .is_ok_and(|page| !page.records.is_empty())
}
