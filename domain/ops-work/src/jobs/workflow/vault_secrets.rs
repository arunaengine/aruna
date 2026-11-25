//! Releases authorized vault secrets for one workflow task attempt into `TaskSpec.vault_env`,
//! checking workflow consumer rules, `secret.use` policies and writing an audit row before open.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::{Secret, SecretBytes};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::NODE_SECRETS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::realm_vault::{RealmSealed, RealmSecretRef, RealmVaultError, RealmVaultRecord};
use aruna_core::request_policy::PolicyRequest;
use aruna_core::secret_registers::{SecretBody, SecretEntry, SecretOwner};
use aruna_core::secrets::{
    Delivery, SecretConsumer, SecretFields, SecretKind, SecretRecord, SecretScope, UseRequest,
    check_use,
};
use aruna_core::structs::execution::job::input::TaskSecretBinding;
use aruna_core::structs::execution::job::local::JobError;
use aruna_core::structs::execution::workflow_run::WorkflowSecretScope;
use aruna_core::structs::identity::admin_audit::{
    AdminAuditAction, AdminAuditRecord, SecretAudit, SecretUse,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_ops_identity::auth::enforcement::{compiled_policies, policy_reads, read_policies};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::secrets::node::{record_key, value_entry};
use aruna_ops_identity::secrets::owner::{EntryReader, sealed_value};
use ulid::Ulid;

use super::credential::RunIdentity;

#[derive(Clone, Debug, PartialEq)]
enum SealedValue {
    Owner(Box<SecretEntry>),
    Realm(RealmVaultRecord),
    Node(Ulid),
}

#[derive(Clone, Debug, PartialEq)]
struct Candidate {
    env_name: String,
    binding: TaskSecretBinding,
    scope: SecretScope,
    kind: SecretKind,
    incarnation: Option<Ulid>,
    value_id: Ulid,
    sealed: SealedValue,
}

#[derive(Debug, PartialEq)]
enum Step {
    Idle,
    OwnerEntry {
        env_name: String,
        binding: TaskSecretBinding,
        reader: EntryReader,
    },
    RealmConfig {
        env_name: String,
        binding: TaskSecretBinding,
    },
    NodeRecord {
        env_name: String,
        binding: TaskSecretBinding,
    },
    Policies(Box<Candidate>),
    Compile(Box<Candidate>),
    Audit(Box<Candidate>),
    RefuseAudit(String),
    Open(Box<Candidate>),
}

/// Releases every vault secret bound to a workflow child job attempt, writing an audit row for
/// every release or refusal before any secret value is opened.
#[derive(Debug, PartialEq)]
pub struct ReleaseVaultSecrets {
    run: RunIdentity,
    run_id: Option<Ulid>,
    pending: Vec<(String, TaskSecretBinding)>,
    released: BTreeMap<String, Secret>,
    ids: IdMint,
    now_ms: u64,
    step: Step,
}

type SecretNext = Result<Next<BTreeMap<String, Secret>>, JobError>;

impl ReleaseVaultSecrets {
    pub fn new(
        run: RunIdentity,
        run_id: Option<Ulid>,
        bindings: BTreeMap<String, TaskSecretBinding>,
    ) -> Self {
        let mut pending: Vec<_> = bindings.into_iter().collect();
        pending.reverse();
        Self {
            run,
            run_id,
            pending,
            released: BTreeMap::new(),
            ids: IdMint::default(),
            now_ms: 0,
            step: Step::Idle,
        }
    }

    fn scope_of(&self, scope: WorkflowSecretScope) -> SecretScope {
        match scope {
            WorkflowSecretScope::User => SecretScope::User(self.run.user_id),
            WorkflowSecretScope::Group => SecretScope::Group(self.run.group_id),
            WorkflowSecretScope::Realm => SecretScope::Realm,
            WorkflowSecretScope::Node => SecretScope::Node,
        }
    }

    fn next_secret(&mut self) -> SecretNext {
        let Some((env_name, binding)) = self.pending.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.released)));
        };
        match binding.scope {
            WorkflowSecretScope::User | WorkflowSecretScope::Group => {
                let owner = match binding.scope {
                    WorkflowSecretScope::User => SecretOwner::User(self.run.user_id),
                    _ => SecretOwner::Group(self.run.group_id),
                };
                let reader = EntryReader::new(self.run.realm_id, &owner, &binding.name);
                let effect = reader.start(None);
                self.step = Step::OwnerEntry {
                    env_name,
                    binding,
                    reader,
                };
                Ok(Next::Call(IoCall::Effect(Box::new(effect))))
            }
            WorkflowSecretScope::Realm => {
                self.step = Step::RealmConfig { env_name, binding };
                let read = GetConfigOperation::new(self.run.realm_id);
                Ok(Next::Call(IoCall::child(read)))
            }
            WorkflowSecretScope::Node => {
                let key = record_key(&binding.name).into();
                self.step = Step::NodeRecord { env_name, binding };
                Ok(Next::Call(IoCall::Read(vec![(
                    NODE_SECRETS_KEYSPACE.to_string(),
                    key,
                )])))
            }
        }
    }

    fn audit_write(
        &mut self,
        binding: &TaskSecretBinding,
        candidate: Option<&Candidate>,
        refused: Option<&str>,
    ) -> Result<IoCall, JobError> {
        let audit_id = self.ids.mint(self.now_ms);
        let action = match refused {
            Some(_) => AdminAuditAction::SecretRefused,
            None => AdminAuditAction::SecretReleased,
        };
        let scope = self.scope_of(binding.scope);
        let secret = SecretAudit {
            scope,
            name: binding.name.clone(),
            incarnation: candidate.and_then(|c| c.incarnation),
            write_id: candidate.map(|c| c.value_id),
        };
        let mut record = AdminAuditRecord::for_secret(audit_id, action, self.run.user_id, secret);
        record.secret_use = Some(SecretUse {
            delivery: Delivery::Value,
            run_id: self.run_id,
            refused: refused.map(str::to_owned),
        });
        let rows = record
            .entries()
            .map_err(|error| JobError::retryable(format!("secret audit encode: {error}")))?;
        Ok(IoCall::Write(rows))
    }

    fn refuse(
        &mut self,
        binding: &TaskSecretBinding,
        candidate: Option<&Candidate>,
        reason: &str,
        message: String,
    ) -> SecretNext {
        let call = self.audit_write(binding, candidate, Some(reason))?;
        self.step = Step::RefuseAudit(message);
        Ok(Next::Call(call))
    }

    fn check_rule(
        &mut self,
        candidate: Candidate,
        rules: &[aruna_core::secrets::UseRule],
    ) -> SecretNext {
        if candidate.binding.scope != WorkflowSecretScope::User {
            let consumer = SecretConsumer::Workflow {
                workflow_id: candidate.binding.workflow_id,
            };
            let request = UseRequest {
                consumer: &consumer,
                delivery: Delivery::Value,
                group: Some(self.run.group_id),
                target: None,
            };
            if let Err(error) = check_use(candidate.scope, rules, &request) {
                let binding = candidate.binding.clone();
                let msg = format!("secret `{}` refused: {error}", binding.name);
                return self.refuse(&binding, Some(&candidate), "rule", msg);
            }
        }
        let reads = policy_reads(self.run.realm_id, Some(self.run.group_id));
        self.step = Step::Policies(Box::new(candidate));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn policy_request(&self, candidate: &Candidate) -> PolicyRequest {
        let realm = self.run.realm_id;
        let name = &candidate.binding.name;
        let path = match candidate.scope {
            SecretScope::Group(group) => format!("/{realm}/g/{group}/secrets/{name}"),
            SecretScope::Realm => format!("/{realm}/secrets/{name}"),
            SecretScope::Node => format!("/{realm}/n/{}/secrets/{name}", self.run.node_id),
            SecretScope::User(user) => format!("/{realm}/u/{}/secrets/{name}", user.user_ulid),
        };
        let params = BTreeMap::from([
            (
                "consumer".to_string(),
                candidate.binding.workflow_id.to_string(),
            ),
            ("delivery".to_string(), "value".to_string()),
            ("origin".to_string(), String::new()),
            ("kind".to_string(), candidate.kind.as_str().to_string()),
        ]);
        PolicyRequest {
            path,
            permission: "read".to_string(),
            user: self.run.user_id.to_string(),
            operation: "secret.use".to_string(),
            params,
            headers: BTreeMap::new(),
            body: None,
            session: None,
        }
    }

    fn open_candidate(&mut self, candidate: Box<Candidate>) -> SecretNext {
        let effect = match &candidate.sealed {
            SealedValue::Owner(entry) => {
                let sealed = sealed_value(self.run.realm_id, entry)
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                StorageEffect::RealmOpen(Box::new(sealed))
            }
            SealedValue::Realm(record) => StorageEffect::RealmOpen(Box::new(RealmSealed::Record {
                realm_id: self.run.realm_id,
                record: record.clone(),
            })),
            SealedValue::Node(value_id) => StorageEffect::VaultRead {
                entry: value_entry(*value_id),
                txn_id: None,
            },
        };
        self.step = Step::Open(candidate);
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
            effect,
        )))))
    }

    fn decode_opened(&mut self, candidate: Candidate, bytes: SecretBytes) -> SecretNext {
        let fields = SecretFields::decode(&bytes)
            .map_err(|_| JobError::permanent("vault secret value could not be decoded"))?;
        let secret = fields
            .resolve_field(&candidate.kind, candidate.binding.field.as_deref())
            .map_err(|_| {
                JobError::permanent(format!(
                    "secret `{}` does not provide the requested field",
                    candidate.binding.name
                ))
            })?;
        self.released.insert(candidate.env_name, secret);
        self.next_secret()
    }
}

impl Operation for ReleaseVaultSecrets {
    type Output = BTreeMap<String, Secret>;
    type Error = JobError;

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> SecretNext {
        self.next_secret()
    }

    fn next(&mut self, result: IoResult) -> SecretNext {
        match std::mem::replace(&mut self.step, Step::Idle) {
            Step::Idle => Err(JobError::retryable(IoError::Unfinished.to_string())),
            Step::OwnerEntry {
                env_name,
                binding,
                mut reader,
            } => {
                let event = result
                    .event()
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                match reader
                    .step(event, None)
                    .map_err(|error| JobError::retryable(error.to_string()))?
                {
                    Some(effect) => {
                        self.step = Step::OwnerEntry {
                            env_name,
                            binding,
                            reader,
                        };
                        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
                    }
                    None => {
                        let Some(entry) = reader.entry().filter(SecretEntry::complete) else {
                            let msg = format!("secret `{}` is missing or incomplete", binding.name);
                            return self.refuse(&binding, None, "entry", msg);
                        };
                        let (Some(info), Some(value), Some(kind)) =
                            (entry.info.as_ref(), entry.value.as_ref(), entry.kind())
                        else {
                            let msg = format!("secret `{}` is incomplete", binding.name);
                            return self.refuse(&binding, None, "entry", msg);
                        };
                        let SecretBody::Info(info_body) = &info.body else {
                            let msg = format!("secret `{}` is malformed", binding.name);
                            return self.refuse(&binding, None, "entry", msg);
                        };
                        let uses = info_body.uses.clone();
                        let candidate = Candidate {
                            env_name,
                            scope: self.scope_of(binding.scope),
                            binding,
                            kind: kind.clone(),
                            incarnation: Some(entry.lifecycle.stamp.write_id),
                            value_id: value.stamp.write_id,
                            sealed: SealedValue::Owner(Box::new(entry)),
                        };
                        self.check_rule(candidate, &uses)
                    }
                }
            }
            Step::RealmConfig { env_name, binding } => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()
                    .map_err(|error| JobError::retryable(error.to_string()))?
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                let found = config
                    .secrets
                    .iter()
                    .find(|record| record.name == binding.name)
                    .and_then(|record| {
                        let entry = RealmSecretRef::named(record.value_id).entry;
                        let vault = config.vault.iter().find(|held| held.entry == entry)?;
                        Some((record.clone(), vault.clone()))
                    });
                let Some((record, vault)) = found else {
                    let msg = format!("realm secret `{}` is missing", binding.name);
                    return self.refuse(&binding, None, "entry", msg);
                };
                let candidate = Candidate {
                    env_name,
                    scope: SecretScope::Realm,
                    binding,
                    kind: record.kind,
                    incarnation: None,
                    value_id: record.value_id,
                    sealed: SealedValue::Realm(vault),
                };
                self.check_rule(candidate, &record.uses)
            }
            Step::NodeRecord { env_name, binding } => {
                let row = result
                    .value()
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                let Some(record) =
                    row.and_then(|bytes| postcard::from_bytes::<SecretRecord>(&bytes).ok())
                else {
                    let msg = format!("node secret `{}` is missing", binding.name);
                    return self.refuse(&binding, None, "entry", msg);
                };
                let candidate = Candidate {
                    env_name,
                    scope: SecretScope::Node,
                    binding,
                    kind: record.kind,
                    incarnation: None,
                    value_id: record.value_id,
                    sealed: SealedValue::Node(record.value_id),
                };
                self.check_rule(candidate, &record.uses)
            }
            Step::Policies(candidate) => {
                let values = result
                    .values()
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                let sets = read_policies(self.run.realm_id, Some(self.run.group_id), values)
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                self.step = Step::Compile(candidate);
                Ok(Next::Call(sets.compile()))
            }
            Step::Compile(candidate) => {
                let policies = compiled_policies(result)
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                let request = self.policy_request(&candidate);
                if policies.evaluate(&request).is_err() {
                    let binding = candidate.binding.clone();
                    let msg = format!("secret `{}` denied by request policy", binding.name);
                    return self.refuse(&binding, Some(&candidate), "policy", msg);
                }
                let binding = candidate.binding.clone();
                let call = self.audit_write(&binding, Some(&candidate), None)?;
                self.step = Step::Audit(candidate);
                Ok(Next::Call(call))
            }
            Step::Audit(candidate) => {
                result
                    .done()
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                self.open_candidate(candidate)
            }
            Step::RefuseAudit(message) => {
                let _ = result.done();
                Err(JobError::permanent(message))
            }
            Step::Open(candidate) => {
                let event = result
                    .event()
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                match (&candidate.sealed, event) {
                    (
                        SealedValue::Node(_),
                        Event::Storage(StorageEvent::VaultResult {
                            secret: Some(bytes),
                            ..
                        }),
                    ) => self.decode_opened(*candidate, bytes),
                    (SealedValue::Node(_), Event::Storage(StorageEvent::VaultResult { .. })) => {
                        Err(JobError::permanent("node vault secret value is missing"))
                    }
                    (_, Event::Storage(StorageEvent::RealmOpened { result: Ok(bytes) })) => {
                        self.decode_opened(*candidate, bytes)
                    }
                    (
                        _,
                        Event::Storage(StorageEvent::RealmOpened {
                            result: Err(RealmVaultError::NoKey),
                        }),
                    ) => Err(JobError::permanent(
                        "vault secrets require a management node or a node-scoped secret",
                    )),
                    (_, Event::Storage(StorageEvent::RealmOpened { result: Err(error) })) => Err(
                        JobError::retryable(format!("could not open vault secret: {error}")),
                    ),
                    (_, other) => Err(JobError::retryable(format!(
                        "unexpected secret storage answer: {}",
                        other.label()
                    ))),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "vault_secrets_tests.rs"]
mod tests;
