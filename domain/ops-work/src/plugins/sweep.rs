//! Lists the active plugin accounts for the sweep that deactivates the ones their install no
//! longer names, after a grace period.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{InstallGuard, PluginInstall};
use aruna_core::plugins::{PluginEffect, PluginEvent};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::user::id::UserId;
use tracing::{info, warn};
use ulid::Ulid;

use super::account::{AccountError, RealmAdmin};
use super::rows::{StoreError, page_call, page_of};
use aruna_ops_identity::users::account_status::{
    AccountStatusConfig, AccountStatusError, AccountStatusOperation,
};

/// How long an account stays unnamed before it is deactivated, so a record still on its way
/// to this node does not cut its account off. Measured in this node's clock.
pub const ORPHAN_GRACE_MS: u64 = 60 * 60 * 1000;
/// Users one sweep page reads.
const PAGE: usize = 1_000;

/// The active plugin accounts with their install, read a page at a time.
pub(crate) fn plugin_accounts() -> PluginAccounts {
    PluginAccounts {
        after: None,
        accounts: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct PluginAccounts {
    after: Option<Vec<u8>>,
    accounts: Vec<(Ulid, User)>,
}

impl PluginAccounts {
    fn page(&self) -> IoCall {
        page_call(USER_KEYSPACE, Vec::new(), self.after.clone(), PAGE)
    }
}

impl Operation for PluginAccounts {
    type Output = Vec<(Ulid, User)>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(self.page()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        let (rows, next) = page_of(answer)?;
        let users = (rows.iter()).filter_map(|(_, value)| User::from_bytes(value).ok());
        self.accounts.extend(
            users
                .filter_map(|user| Some((user.install()?, user)))
                .filter(|(_, user)| !user.is_deactivated()),
        );
        Ok(match next {
            Some(key) => {
                self.after = Some(key);
                Next::Call(self.page())
            }
            None => Next::Done(std::mem::take(&mut self.accounts)),
        })
    }
}

/// The accounts no record names, and the ones a record names, which forget when they were
/// first seen unnamed.
fn split_accounts(
    config: &RealmConfigDocument,
    accounts: Vec<(Ulid, User)>,
) -> (Vec<(Ulid, UserId)>, Vec<UserId>) {
    let (mut unnamed, mut named) = (Vec::new(), Vec::new());
    for (install_id, user) in accounts {
        let names = |install: &&PluginInstall| {
            install.install_id == install_id && install.account == user.user_id
        };
        match config.installs.iter().any(|install| names(&install)) {
            true => named.push(user.user_id),
            false => unnamed.push((install_id, user.user_id)),
        }
    }
    (unnamed, named)
}

/// The call that asks the plugin host when each unnamed account was first seen.
fn first_seen(unnamed: &[(Ulid, UserId)], named: Vec<UserId>, now_ms: u64) -> IoCall {
    let unnamed = unnamed.iter().map(|(_, account)| *account).collect();
    let effect = Effect::Plugin(PluginEffect::FirstSeen {
        named,
        unnamed,
        now_ms,
    });
    IoCall::Effect(Box::new(effect))
}

/// The unnamed accounts that have stayed unnamed for the grace period, from when the plugin
/// host first saw each; a restart starts the grace again.
fn due_accounts(
    unnamed: Vec<(Ulid, UserId)>,
    result: IoResult,
    now_ms: u64,
) -> Result<VecDeque<(Ulid, UserId)>, IoError> {
    let expected = "Event::Plugin(PluginEvent::Seen)";
    let IoResult::Event(event) = result else {
        let got = result.label();
        return Err(IoError::Unexpected { expected, got });
    };
    let Event::Plugin(PluginEvent::Seen(seen)) = *event else {
        let got = event.label();
        return Err(IoError::Unexpected { expected, got });
    };
    Ok((unnamed.into_iter().zip(seen))
        .filter(|(_, first)| now_ms.saturating_sub(*first) >= ORPHAN_GRACE_MS)
        .map(|(account, _)| account)
        .collect())
}

#[derive(Debug, PartialEq)]
enum SweepPhase {
    Accounts,
    Seen(Vec<(Ulid, UserId)>),
    Admin(VecDeque<(Ulid, UserId)>),
    Status(UserId, VecDeque<(Ulid, UserId)>),
}

/// Deactivates the active plugin accounts the canonical records have not named for the grace
/// period, as a realm administrator. Answers how many it deactivated.
#[derive(Debug, PartialEq)]
pub(crate) struct SweepOrphans {
    config: Arc<RealmConfigDocument>,
    node_id: NodeId,
    now_ms: u64,
    phase: SweepPhase,
    current: Option<(Ulid, UserId)>,
    swept: usize,
}

impl SweepOrphans {
    pub(crate) fn new(config: Arc<RealmConfigDocument>, node_id: NodeId, now_ms: u64) -> Self {
        Self {
            config,
            node_id,
            now_ms,
            phase: SweepPhase::Accounts,
            current: None,
            swept: 0,
        }
    }

    fn advance(&mut self, admin: UserId, mut due: VecDeque<(Ulid, UserId)>) -> Next<usize> {
        let Some((install_id, account)) = due.pop_front() else {
            return Next::Done(self.swept);
        };
        let realm_id = self.config.realm_id;
        let status = AccountStatusOperation::new(AccountStatusConfig {
            actor: Actor {
                node_id: self.node_id,
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
            now: self.now_ms / 1000,
        })
        .guarded(InstallGuard::Unnamed {
            install_id,
            account,
        })
        .by_install();
        self.current = Some((install_id, account));
        self.phase = SweepPhase::Status(admin, due);
        Next::Call(IoCall::child(status))
    }
}

impl Operation for SweepOrphans {
    type Output = usize;
    type Error = AccountError;

    fn first(&mut self) -> Result<Next<usize>, AccountError> {
        Ok(Next::Call(IoCall::child(plugin_accounts())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, AccountError> {
        match std::mem::replace(&mut self.phase, SweepPhase::Accounts) {
            SweepPhase::Accounts => {
                let accounts = result.child::<Vec<(Ulid, User)>, StoreError>()??;
                let (unnamed, named) = split_accounts(&self.config, accounts);
                let seen = first_seen(&unnamed, named, self.now_ms);
                self.phase = SweepPhase::Seen(unnamed);
                Ok(Next::Call(seen))
            }
            SweepPhase::Seen(unnamed) => {
                let due = due_accounts(unnamed, result, self.now_ms)?;
                if due.is_empty() {
                    return Ok(Next::Done(0));
                }
                self.phase = SweepPhase::Admin(due);
                Ok(Next::Call(IoCall::child(RealmAdmin(self.config.realm_id))))
            }
            SweepPhase::Admin(due) => match result.child::<Option<UserId>, AccountError>()?? {
                Some(admin) => Ok(self.advance(admin, due)),
                None => {
                    warn!("No realm administrator to deactivate unnamed plugin accounts with");
                    Ok(Next::Done(0))
                }
            },
            SweepPhase::Status(admin, due) => {
                let (install_id, account) = self.current.take().ok_or(IoError::Unfinished)?;
                match result.child::<(), AccountStatusError>()? {
                    Ok(()) => {
                        let done = "Deactivated a plugin account its install no longer names";
                        info!(%account, %install_id, "{done}");
                        self.swept += 1;
                    }
                    Err(error) => {
                        warn!(%account, %error, "Could not deactivate an unnamed plugin account");
                    }
                }
                Ok(self.advance(admin, due))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::calls::*;
    use crate::plugins::fixtures;
    use crate::plugins::rows::tests::{child_result, page_result};
    use crate::plugins::test_support::{install_as, install_request};
    use aruna_core::user::id::UserId;
    use aruna_test_harness::plugins::Fixture;

    #[tokio::test]
    async fn unnamed_accounts() {
        let fixture = Fixture::new().await;
        let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
        let installed = install_as(&fixture, &install_request(&fixture, None))
            .await
            .unwrap();
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

    fn orphan_sweep() -> SweepOrphans {
        let config = fixtures::config(&fixtures::install(), serde_json::json!({}));
        SweepOrphans::new(Arc::new(config), fixtures::node(3), 5_000)
    }

    fn due_one() -> VecDeque<(Ulid, UserId)> {
        let install = fixtures::install();
        VecDeque::from([(install.install_id, install.account)])
    }

    fn seen(times: Vec<u64>) -> IoResult {
        IoResult::Event(Box::new(Event::Plugin(PluginEvent::Seen(times))))
    }

    #[test]
    fn sweep_without_accounts() {
        let mut sweep = orphan_sweep();
        assert!(fixtures::is_child(&sweep.first()));
        let none = child_result(Ok::<_, StoreError>(Vec::<(Ulid, User)>::new()));
        let asked = sweep.next(none);
        assert!(matches!(asked, Ok(Next::Call(IoCall::Effect(_)))));
        assert_eq!(sweep.next(seen(Vec::new())), Ok(Next::Done(0)));
    }

    #[test]
    fn sweep_waits_grace() {
        // An account first seen unnamed now waits the grace period before it is swept.
        let install = fixtures::install();
        let mut sweep = orphan_sweep();
        sweep.phase = SweepPhase::Seen(vec![(install.install_id, install.account)]);
        assert_eq!(sweep.next(seen(vec![5_000])), Ok(Next::Done(0)));
        let mut late = orphan_sweep();
        late.now_ms = ORPHAN_GRACE_MS + 5_000;
        late.phase = SweepPhase::Seen(vec![(install.install_id, install.account)]);
        assert!(fixtures::is_child(&late.next(seen(vec![5_000]))));
    }

    #[test]
    fn sweep_needs_admin() {
        let mut sweep = orphan_sweep();
        sweep.phase = SweepPhase::Admin(due_one());
        let none = child_result(Ok::<Option<UserId>, AccountError>(None));
        assert_eq!(sweep.next(none), Ok(Next::Done(0)));
    }

    #[test]
    fn sweep_counts_deactivated() {
        let admin = fixtures::install().granted_by;
        let mut sweep = orphan_sweep();
        sweep.phase = SweepPhase::Admin(due_one());
        let found = child_result(Ok::<_, AccountError>(Some(admin)));
        assert!(fixtures::is_child(&sweep.next(found)));
        let done = child_result(Ok::<(), AccountStatusError>(()));
        assert_eq!(sweep.next(done), Ok(Next::Done(1)));
    }

    #[test]
    fn accounts_page_through() {
        let mut plan = PluginAccounts {
            after: None,
            accounts: Vec::new(),
        };
        assert!(matches!(
            plan.first().unwrap(),
            Next::Call(IoCall::Page { after: None, .. })
        ));
        let rows = vec![(b"u1".to_vec(), Vec::new())];
        let Next::Call(IoCall::Page { after, .. }) =
            plan.next(page_result(rows, Some(b"u1".to_vec()))).unwrap()
        else {
            panic!("a next key reads the next page");
        };
        assert_eq!(after.map(|key| key.to_vec()), Some(b"u1".to_vec()));
        let Next::Done(accounts) = plan.next(page_result(Vec::new(), None)).unwrap() else {
            panic!("the last page ends the listing");
        };
        assert!(accounts.is_empty());
        assert!(plan.next(IoResult::Done).is_err());
    }
}
