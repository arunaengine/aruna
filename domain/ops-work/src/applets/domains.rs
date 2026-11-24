//! Checks a custom applet domain in DNS: its `_aruna-verify` TXT value and that it points to
//! the applet host. A domain another applet already serves never passes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::hosts::{domain_proven, host_key, verify_name};
use aruna_core::applet::record::Applet;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::APPLET_HOSTS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::outbound::{DnsAnswer, DnsQuery, OutboundEffect, OutboundEvent};
use aruna_core::structs::identity::auth::AuthContext;
use ulid::Ulid;

use super::change::{AppletChange, WriteChangeOperation};
use super::{AppletAccessOperation, AppletError, Need, applet_answer, host_answer, read_applet};

#[derive(Debug, PartialEq)]
enum CheckStep {
    Record,
    Dns,
    Claim,
    Write,
}

/// Checks `host` of `applet_id` once and records the result; two failures in a row suspend
/// an active domain. With a caller, it needs WRITE on the group's data; the node checks alone.
#[derive(Debug, PartialEq)]
pub struct VerifyDomainOperation {
    auth: Option<AuthContext>,
    applet_id: Ulid,
    host: String,
    applet: Option<Applet>,
    proven: bool,
    step: CheckStep,
}

impl VerifyDomainOperation {
    pub fn new(auth: Option<&AuthContext>, (applet_id, host): (Ulid, &str)) -> Self {
        Self {
            auth: auth.cloned(),
            applet_id,
            host: host.to_string(),
            applet: None,
            proven: false,
            step: CheckStep::Record,
        }
    }

    fn lookup(&mut self, applet: Applet) -> Result<Next<Applet>, AppletError> {
        let domain = (applet.domains.iter())
            .find(|domain| domain.host == self.host)
            .ok_or(AppletError::NotFound)?;
        let query = DnsQuery {
            txt: verify_name(&domain.host),
            hosts: vec![domain.host.clone(), applet.host.clone()],
        };
        self.applet = Some(applet);
        self.step = CheckStep::Dns;
        let effect = Effect::Outbound(OutboundEffect::Dns(Box::new(query)));
        Ok(Next::Call(IoCall::from(effect)))
    }

    fn proven(&self, answer: &DnsAnswer) -> Result<bool, AppletError> {
        let applet = self.applet.as_ref().ok_or(IoError::Unfinished)?;
        let nonce = (applet.domains.iter())
            .find(|domain| domain.host == self.host)
            .map(|domain| domain.nonce.as_str())
            .ok_or(AppletError::NotFound)?;
        let empty = Vec::new();
        let domain = answer.addresses.first().unwrap_or(&empty);
        let own = answer.addresses.get(1).unwrap_or(&empty);
        Ok(domain_proven(&answer.txt, nonce, (domain, own)))
    }

    fn record(&mut self) -> Result<Next<Applet>, AppletError> {
        let applet = self.applet.as_ref().ok_or(IoError::Unfinished)?;
        // A check the node makes alone is recorded under the applet's creator.
        let actor = (self.auth.as_ref()).map_or(applet.created_by, |auth| auth.user_id);
        let change = AppletChange::CheckDomain {
            host: self.host.clone(),
            passed: self.proven,
        };
        let write = WriteChangeOperation::node(self.applet_id, change, actor);
        self.step = CheckStep::Write;
        Ok(Next::Call(IoCall::retry(write, &self.applet_id.to_bytes())))
    }
}

impl Operation for VerifyDomainOperation {
    type Output = Applet;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<Applet>, AppletError> {
        Ok(Next::Call(match &self.auth {
            Some(auth) => IoCall::child(AppletAccessOperation::new(
                auth,
                self.applet_id,
                Need::Write,
            )),
            None => read_applet(self.applet_id),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AppletError> {
        match self.step {
            CheckStep::Record => {
                let applet = match self.auth {
                    Some(_) => result.child::<Applet, AppletError>()??,
                    None => applet_answer(result)?.ok_or(AppletError::NotFound)?,
                };
                self.lookup(applet)
            }
            CheckStep::Dns => {
                let answer = match result.event()? {
                    Event::Outbound(OutboundEvent::DnsAnswered { result }) => result,
                    other => {
                        return Err(AppletError::from(IoError::Unexpected {
                            expected: "OutboundEvent::DnsAnswered",
                            got: other.label(),
                        }));
                    }
                };
                // Nothing could be asked, so the check says nothing about the domain.
                let answer = answer.map_err(|error| AppletError::Storage(error.to_string()))?;
                self.proven = self.proven(&answer)?;
                if !self.proven {
                    return self.record();
                }
                self.step = CheckStep::Claim;
                let row = (APPLET_HOSTS_KEYSPACE.to_string(), host_key(&self.host));
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
            CheckStep::Claim => {
                let row = host_answer(result.value()?)?;
                self.proven = row.is_none_or(|row| row.applet_id == self.applet_id);
                self.record()
            }
            CheckStep::Write => result.child::<Applet, AppletError>()?.map(Next::Done),
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}
