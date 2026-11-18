//! Answers Git store calls with the store the blob handle owns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::{GitAnswer, GitCall};

use crate::driver::DriverContext;

pub(super) async fn dispatch_git(call: GitCall, context: &DriverContext) -> GitAnswer {
    let store = context.blob_handle.as_ref().and_then(|handle| handle.git());
    let Some(store) = store else {
        return GitAnswer::Failed("this node has no Git store".to_string());
    };
    match call {
        GitCall::Run { effect, actor } => match store.execute(effect, actor).await {
            Ok(event) => GitAnswer::Ran(event),
            Err(error) => GitAnswer::Failed(error.to_string()),
        },
        GitCall::Configured(document_id) => {
            GitAnswer::Configured(store.configured(document_id).await)
        }
        GitCall::Busy(document_id) => GitAnswer::Busy(store.try_lock(document_id).is_none()),
    }
}
