//! Tests the inbound frame read timeouts and their budget.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::metadata::handle::transport::read_budget;

async fn assert_timeout(frame: &[u8], held: bool) {
    let (mut writer, reader) = tokio::io::duplex(16);
    writer.write_all(frame).await.unwrap();
    let budget = Arc::new(tokio::sync::Semaphore::new(8));
    let task_budget = budget.clone();
    let task = tokio::spawn(async move {
        let mut reader = reader;
        read_budget(&mut reader, &task_budget).await
    });
    tokio::task::yield_now().await;
    assert_eq!(budget.available_permits(), if held { 0 } else { 8 });

    tokio::time::advance(METADATA_IO_TIMEOUT).await;
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        MetadataError::Backend(message)
            if message == "timed out waiting for metadata message"
    ));
    assert_eq!(budget.available_permits(), 8);
}
#[tokio::test(start_paused = true)]
async fn header_timeout() {
    assert_timeout(&[0], false).await;
}
#[tokio::test(start_paused = true)]
async fn body_timeout() {
    assert_timeout(&[0, 0, 0, 0, 8], true).await;
}
