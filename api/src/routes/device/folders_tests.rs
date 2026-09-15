use super::{
    FolderError, ServerError, folder_name, map_action_error, map_folder_error, map_reconcile_error,
    parse_hash,
};
use crate::routes::device::dto::hex_hash;
use aruna_core::id::NodeId;
use aruna_core::structs::{RemoteBinding, SyncRefusal};
use aruna_operations::device::sync::ReconcileFolderError;
use aruna_operations::device::sync::actions::ActionError;
use axum::http::StatusCode;

#[test]
fn round_trips_hashes() {
    let hash = [0xabu8; 32];
    assert_eq!(parse_hash(&hex_hash(&hash)).unwrap(), hash);
    assert_eq!(parse_hash(&hex_hash(&hash).to_uppercase()).unwrap(), hash);
    assert!(parse_hash("nothex").is_err());
    assert!(parse_hash(&"z".repeat(64)).is_err());
}

#[test]
fn rejects_malformed_hashes() {
    // A 3-byte scalar keeps the byte length at 64 while breaking char boundaries.
    let malformed = format!("€{}", "a".repeat(61));
    assert_eq!(malformed.len(), 64);
    assert!(parse_hash(&malformed).is_err());
    assert!(parse_hash(&"a".repeat(62)).is_err());
    assert!(parse_hash(&"a".repeat(66)).is_err());
}

#[test]
fn names_folder_root() {
    // The confirmation is the directory's own name, on either platform.
    assert_eq!(folder_name("/home/ada/data"), "data");
    assert_eq!(folder_name("/home/ada/data/"), "data");
    assert_eq!(folder_name(r"C:\Users\ada\Data"), "Data");
    assert_eq!(folder_name("data"), "data");
}

#[test]
fn maps_folder_errors() {
    // A nested root is the owner's problem, never an internal fault.
    assert!(matches!(
        map_folder_error(FolderError::RootOverlaps("/home/ada".to_string())),
        ServerError::Conflict(_)
    ));
    assert!(matches!(
        map_folder_error(FolderError::NotFound),
        ServerError::NotFound
    ));
    assert!(matches!(
        map_action_error(ActionError::ExpectedMissing),
        ServerError::BadRequestReason(_)
    ));
    assert!(matches!(
        map_action_error(ActionError::RemoteUnavailable),
        ServerError::ServiceUnavailableReason(_)
    ));

    let node = NodeId::from_bytes(&[3u8; 32]).expect("node id");
    let missing = map_folder_error(FolderError::RemoteBucketMissing {
        node,
        bucket: "test".to_string(),
    });
    assert_eq!(missing.status_code(), StatusCode::BAD_REQUEST);
    assert_eq!(
        missing.to_string(),
        format!("the bucket \"test\" does not exist on node {node}")
    );
    let conflict = map_folder_error(FolderError::RemoteBucketConflict {
        bucket: "test".to_string(),
        reason: "bucket \"test\" belongs to another group".to_string(),
    });
    assert_eq!(conflict.status_code(), StatusCode::CONFLICT);
    let remote = RemoteBinding {
        node_id: node,
        bucket: "test".to_string(),
        prefix: String::new(),
    };
    let sync = map_reconcile_error(
        ReconcileFolderError::Refused(SyncRefusal::NotFound),
        &remote,
    );
    assert_eq!(sync.status_code(), StatusCode::BAD_REQUEST);
    assert_eq!(sync.to_string(), missing.to_string());
    let target = map_folder_error(FolderError::NotRealmNode(node));
    assert_eq!(target.status_code(), StatusCode::BAD_REQUEST);
    assert_eq!(
        target.to_string(),
        format!("node {node} is not a realm server")
    );
}
