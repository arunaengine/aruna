//! Hands file server calls to the node's VFS adapter. A node without one lets vault transitions
//! through and answers every other call as unconfigured.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{VfsEffect, VfsEvent, VfsFailure};
use ulid::Ulid;

use crate::driver::DriverContext;

pub(super) async fn dispatch_vfs(effect: VfsEffect, context: &DriverContext) -> VfsEvent {
    match (context.vfs_handle.as_ref(), effect) {
        (Some(handle), effect) => handle.execute(effect).await,
        // No file server holds plaintext stages here, so nothing can block the transition.
        (None, VfsEffect::FenceVault { .. }) => VfsEvent::VaultFenced {
            fence_id: Ulid::nil(),
        },
        (None, VfsEffect::ReleaseFence { .. }) => VfsEvent::Done,
        (None, _) => VfsEvent::Failed(VfsFailure::Unconfigured),
    }
}
