//! Reads automation records and paged group lists from node-local storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::record::{Automation, Lifecycle};
use aruna_core::calls::{IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::{AutoError, allowed, auto_answer, autos_page, group_read, read_auto};

/// Loads one automation record without caller auth checks (for internal runner and matcher).
#[derive(Debug, PartialEq)]
pub struct LoadAutoOperation {
    id: Ulid,
}

impl LoadAutoOperation {
    pub fn new(id: Ulid) -> Self {
        Self { id }
    }
}

impl Operation for LoadAutoOperation {
    type Output = Option<Automation>;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Self::Output>, AutoError> {
        Ok(Next::Call(read_auto(self.id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, AutoError> {
        Ok(Next::Done(auto_answer(result)?))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AutoPage {
    pub items: Vec<Automation>,
    pub next: Option<Ulid>,
}

/// Pages automations belonging to `group_id` for a caller with READ on the group.
#[derive(Debug, PartialEq)]
pub struct ListAutosOperation {
    auth: AuthContext,
    group_id: GroupId,
    local: NodeId,
    lifecycle: Option<Lifecycle>,
    after: Option<Ulid>,
    limit: usize,
    authorized: bool,
    found: Vec<Automation>,
}

impl ListAutosOperation {
    pub fn new(
        auth: &AuthContext,
        (group_id, local): (GroupId, NodeId),
        lifecycle: Option<Lifecycle>,
        after: Option<Ulid>,
        limit: usize,
    ) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            local,
            lifecycle,
            after,
            limit: limit.clamp(1, 500),
            authorized: false,
            found: Vec::new(),
        }
    }
}

impl Operation for ListAutosOperation {
    type Output = AutoPage;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<AutoPage>, AutoError> {
        Ok(Next::Call(group_read(
            &self.auth,
            self.group_id,
            self.local,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AutoPage>, AutoError> {
        if !self.authorized {
            if !allowed(result)? {
                return Err(AutoError::NotFound);
            }
            self.authorized = true;
            return Ok(Next::Call(autos_page(None)));
        }
        let (rows, next) = result.page()?;
        for (_, value) in rows {
            let item = Automation::from_bytes(&value)?;
            if item.group_id == self.group_id
                && item.lifecycle != Lifecycle::Removed
                && self.lifecycle.is_none_or(|want| item.lifecycle == want)
            {
                self.found.push(item);
            }
        }
        if let Some(after) = next {
            return Ok(Next::Call(autos_page(Some(after))));
        }
        let mut items = std::mem::take(&mut self.found);
        items.sort_by_key(|item| item.automation_id);
        if let Some(after) = self.after {
            items.retain(|item| item.automation_id > after);
        }
        let has_more = items.len() > self.limit;
        items.truncate(self.limit);
        let cursor = has_more
            .then(|| items.last().map(|item| item.automation_id))
            .flatten();
        Ok(Next::Done(AutoPage {
            items,
            next: cursor,
        }))
    }

    fn expected_error(error: &AutoError) -> bool {
        matches!(error, AutoError::NotFound)
    }
}
