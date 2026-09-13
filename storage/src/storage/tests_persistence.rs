use super::tests::{
    assert_read_result, assert_write_result, commit_transaction, start_write_transaction,
};
use super::{FjallPersistPolicy, FjallStorage};
use aruna_core::effects::StorageEffect;
use std::{env, process::Command};
use tempfile::tempdir;

const RESTART_CHILD_PATH_ENV: &str = "ARUNA_STORAGE_RESTART_CHILD_PATH";
const RESTART_CHILD_MODE_ENV: &str = "ARUNA_STORAGE_RESTART_CHILD_MODE";
const RESTART_CHILD_TEST: &str = "storage::tests_persistence::persistence_restart_child";

fn run_restart_child(mode: &str, path: &str) {
    let status = Command::new(env::current_exe().expect("test binary path"))
        .arg(RESTART_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env(RESTART_CHILD_PATH_ENV, path)
        .env(RESTART_CHILD_MODE_ENV, mode)
        .status()
        .expect("restart child process should run");

    assert!(status.success(), "restart child process failed: {status}");
}

#[test]
fn persistence_restart_child() {
    let Ok(mode) = env::var(RESTART_CHILD_MODE_ENV) else {
        return;
    };
    let path = env::var(RESTART_CHILD_PATH_ENV).expect("restart child path");
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    runtime.block_on(async {
        let handle = FjallStorage::open_with_policy(&path, FjallPersistPolicy::Buffer)
            .expect("storage opens in restart child");

        match mode.as_str() {
            "write" => {
                assert_write_result(
                    handle
                        .send_storage_effect(StorageEffect::Write {
                            key_space: "restart_write".to_string(),
                            key: b"key".to_vec().into(),
                            value: b"value".to_vec().into(),
                            txn_id: None,
                        })
                        .await,
                    b"key",
                );
            }
            "transaction" => {
                let txn_id = start_write_transaction(&handle).await;
                assert_write_result(
                    handle
                        .send_storage_effect(StorageEffect::Write {
                            key_space: "restart_transaction".to_string(),
                            key: b"key".to_vec().into(),
                            value: b"transaction".to_vec().into(),
                            txn_id: Some(txn_id),
                        })
                        .await,
                    b"key",
                );
                commit_transaction(&handle, txn_id).await;
            }
            other => panic!("unsupported restart child mode: {other}"),
        }
    });

    // Skip Rust destructors so the parent verifies the restart contract, not shutdown cleanup.
    std::process::exit(0);
}

#[tokio::test]
async fn buffered_write_survives() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    run_restart_child("write", path);
    let handle = FjallStorage::open_with_policy(path, FjallPersistPolicy::Buffer)
        .expect("storage reopens after restart");

    assert_read_result(
        handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "restart_write".to_string(),
                key: b"key".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"key",
        b"value",
    );
}

#[tokio::test]
async fn buffered_commit_survives() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    run_restart_child("transaction", path);
    let handle = FjallStorage::open_with_policy(path, FjallPersistPolicy::Buffer)
        .expect("storage reopens after restart");

    assert_read_result(
        handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "restart_transaction".to_string(),
                key: b"key".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"key",
        b"transaction",
    );
}
