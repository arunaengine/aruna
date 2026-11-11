//! Deactivates plugin accounts that their install no longer names, after a grace period. The
//! write is guarded so it applies only while the install record still does not name them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use aruna_core::keyspaces::{AUTH_KEYSPACE, USER_KEYSPACE};
use aruna_core::plugin_installs::InstallGuard;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_secs;
use aruna_core::{NodeId, UserId};
use tracing::{info, warn};

use super::account::read_row;
use crate::driver::{DriverContext, drive};
use crate::storage_read::scan_all;
use crate::users::account_status::{AccountStatusConfig, AccountStatusOperation, REALM_ADMIN_ROLE};

/// How long an account stays unnamed before it is deactivated, so a record still on its way
/// to this node does not cut its account off. Measured in this node's clock.
pub const ORPHAN_GRACE_MS: u64 = 60 * 60 * 1000;

/// When this node first saw each unnamed account; a restart starts the grace again.
static SEEN: LazyLock<Mutex<HashMap<UserId, u64>>> = LazyLock::new(Default::default);

/// Deactivates the active plugin accounts the canonical records have not named for the grace
/// period, as a realm administrator. Returns how many it deactivated.
pub async fn sweep_orphans(
    ctx: &DriverContext,
    node_id: NodeId,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, String> {
    let rows = scan_all(&ctx.storage_handle, USER_KEYSPACE, None).await?;
    let accounts = (rows.iter())
        .filter_map(|(_, value)| User::from_bytes(value).ok())
        .filter_map(|user| Some((user.install()?, user)))
        .filter(|(_, user)| !user.is_deactivated());
    let mut due = Vec::new();
    {
        let mut seen = SEEN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        for (install_id, user) in accounts {
            let named = (config.installs.iter())
                .any(|install| install.install_id == install_id && install.account == user.user_id);
            if named {
                seen.remove(&user.user_id);
                continue;
            }
            let first = *seen.entry(user.user_id).or_insert(now_ms);
            if now_ms.saturating_sub(first) >= ORPHAN_GRACE_MS {
                due.push((install_id, user.user_id));
            }
        }
    }
    if due.is_empty() {
        return Ok(0);
    }
    let Some(admin) = realm_admin(ctx, config).await? else {
        warn!("No realm administrator to deactivate unnamed plugin accounts with");
        return Ok(0);
    };
    let realm_id = config.realm_id;
    let mut swept = 0;
    for (install_id, account) in due {
        let status = AccountStatusOperation::new(AccountStatusConfig {
            actor: Actor {
                node_id,
                user_id: admin,
                realm_id,
            },
            auth_context: AuthContext {
                user_id: admin,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            target: account,
            active: false,
            now: unix_timestamp_secs(),
        })
        .guarded(InstallGuard::Unnamed {
            install_id,
            account,
        })
        .by_install();
        match drive(status, ctx).await {
            Ok(()) => {
                info!(%account, %install_id, "Deactivated a plugin account its install no longer names");
                swept += 1;
            }
            Err(error) => warn!(%account, %error, "Could not deactivate an unnamed plugin account"),
        }
    }
    Ok(swept)
}

/// The realm administrator with the smallest id, so every node acts as the same one.
async fn realm_admin(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
) -> Result<Option<UserId>, String> {
    let key = config.realm_id.as_bytes().to_vec();
    let realm = read_row(ctx, AUTH_KEYSPACE, key).await?;
    let realm = (realm.map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes)))
        .transpose()
        .map_err(|error| error.to_string())?;
    let admins = (realm.iter().flat_map(|realm| realm.roles.values()))
        .filter(|role| role.name == REALM_ADMIN_ROLE)
        .flat_map(|role| role.assigned_users.iter().copied());
    Ok(admins.min())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::install::tests::Fixture;
    use crate::plugins::install::{read_config, write_record};

    #[tokio::test]
    async fn unnamed_accounts() {
        let fixture = Fixture::new().await;
        let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
        let installed = fixture.install(&fixture.request(None)).await.unwrap();
        let config = read_config(ctx, actor.realm_id).await.unwrap();
        let now = 1_000;
        assert_eq!(
            sweep_orphans(ctx, actor.node_id, &config, now)
                .await
                .unwrap(),
            0
        );

        // The record names a new account; the old one is swept only after the grace period.
        let mut moved = installed.clone();
        moved.account = UserId::local(ulid::Ulid::from_parts(9, 9), actor.realm_id);
        moved.revision += 1;
        write_record(ctx, actor, &auth, Some(installed.revision), moved, None)
            .await
            .unwrap();
        let config = read_config(ctx, actor.realm_id).await.unwrap();
        assert_eq!(
            sweep_orphans(ctx, actor.node_id, &config, now)
                .await
                .unwrap(),
            0
        );
        let later = now + ORPHAN_GRACE_MS;
        assert_eq!(
            sweep_orphans(ctx, actor.node_id, &config, later)
                .await
                .unwrap(),
            1
        );
        let user = fixture.account(&installed).await.unwrap();
        assert!(user.stopped_by_install());
        assert_eq!(
            sweep_orphans(ctx, actor.node_id, &config, later)
                .await
                .unwrap(),
            0
        );
    }
}
