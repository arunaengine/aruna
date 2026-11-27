//! Hands file server calls to the node's VFS adapter. A node without one lets vault transitions
//! and grant revocations through and answers every other call as unconfigured.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{VfsEffect, VfsEvent, VfsFailure};
use ulid::Ulid;

use crate::driver::DriverContext;

pub(super) async fn dispatch_vfs(effect: VfsEffect, context: &DriverContext) -> VfsEvent {
    match (context.vfs_handle.as_ref(), effect) {
        (Some(handle), effect) => handle.execute(effect).await,
        // No handle means both listeners are off and startup found no stage folder, since
        // `open_vfs` opens one whenever it exists. No plaintext scratch file can block it.
        (None, VfsEffect::FenceVault { .. }) => VfsEvent::VaultFenced {
            fence_id: Ulid::nil(),
        },
        (None, VfsEffect::ReleaseFence { .. }) => VfsEvent::Done,
        // Nothing is served here, so no session or open can hold a revoked grant.
        (None, VfsEffect::RevokeGrant { .. }) => VfsEvent::Done,
        (None, _) => VfsEvent::Failed(VfsFailure::Unconfigured),
    }
}
