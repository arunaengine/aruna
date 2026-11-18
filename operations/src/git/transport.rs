//! Serves Git over HTTP from this holder's repository cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::{GitEffect, GitEvent, GitRequest};
use aruna_core::structs::identity::auth::{AuthContext, Permission};

use super::push::PushKey;
use super::{GitError, pending, repository, snapshot, store};
use crate::driver::{DriverContext, drive};

/// Serves Git over HTTP after bringing the local cache up to date from the records.
pub async fn transport(
    context: &DriverContext,
    auth: &AuthContext,
    request: GitRequest,
) -> Result<GitEvent, GitError> {
    let store = store(context)?;
    let permission =
        if request.action == "git-receive-pack" || request.query == "service=git-receive-pack" {
            Permission::WRITE
        } else {
            Permission::READ
        };
    let write = permission == Permission::WRITE;
    let (document, repository) = drive(
        repository(
            context.node_id(),
            auth,
            request.repository.document_id,
            permission,
        ),
        context,
    )
    .await?;
    if repository != request.repository {
        return Err(GitError::Conflict);
    }
    let mut request = request;
    let guard = match write {
        true => {
            let guard = store.lock(document.document_id).await;
            snapshot::refresh(context, store, &document).await?;
            Some(guard)
        }
        false => {
            snapshot::read_view(context, store, &document).await?;
            None
        }
    };
    // Reads need a current cache but not the lock; pushes keep it until their record is out.
    let _guard = guard;
    let _key = write.then(|| {
        let key = PushKey::open(document.document_id);
        request.push_key = key.value.clone();
        key
    });
    let served = store
        .execute(GitEffect::Http(Box::new(request)), auth.user_id)
        .await
        .map_err(|_| GitError::Unavailable)?;
    // A recorded push to main brings metadata; it applies now, or on a later refresh.
    if write && let Err(error) = pending::apply(context, store, &document).await {
        tracing::warn!(document_id = %document.document_id, %error, "Pushed metadata waits");
    }
    Ok(served)
}
