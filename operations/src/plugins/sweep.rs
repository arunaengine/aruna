//! Lists the active plugin accounts for the sweep that deactivates the ones their install no
//! longer names, after a grace period.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::user::User;
use ulid::Ulid;

use super::rows::{StoreError, page_call, page_of};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::install::sweep_orphans;
    use crate::plugins::install::tests::Fixture;
    use crate::plugins::install::{read_config, write_record};
    use crate::plugins::rows::page_result;
    use aruna_core::UserId;

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
        assert_eq!(after, Some(b"u1".to_vec()));
        let Next::Done(accounts) = plan.next(page_result(Vec::new(), None)).unwrap() else {
            panic!("the last page ends the listing");
        };
        assert!(accounts.is_empty());
        assert!(plan.next(IoResult::Done).is_err());
    }
}
