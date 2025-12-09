use crate::config::{Config, RuleTarget};
use anyhow::anyhow;
use anyhow::Result;
use cel_interpreter::Context;
use cel_interpreter::Value;
use cel_parser::ast::Expression;
use tracing::error;

use super::rule_structs::BundleRuleInput;
use super::rule_structs::ObjectRuleInput;
use super::rule_structs::PackageObjectRuleInput;
use super::rule_structs::ReplicationIncomingRuleInput;
use super::rule_structs::ReplicationOutgoingRuleInput;
use super::rule_structs::RootRuleInput;

#[allow(dead_code)]
pub(super) struct RuleEngine {
    root: Vec<Expression>,
    obj: Vec<Expression>,
    bundle: Vec<Expression>,
    obj_package: Vec<Expression>,
    repl_in: Vec<Expression>,
    repl_out: Vec<Expression>,
}

impl RuleEngine {
    pub fn new(config: &Config) -> Result<Self> {
        let mut root_expressions = vec![];
        let mut object_expressions = vec![];
        let mut bundle_expressions = vec![];
        let mut object_package_expressions = vec![];
        let mut replication_inbound_expressions = vec![];
        let mut replication_outbound_expressions = vec![];
        for rule in config.get_rules().iter() {
            match rule.target {
                RuleTarget::ROOT => {
                    root_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::OBJECT => {
                    object_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::BUNDLE => {
                    bundle_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::OBJECTPACKAGE => {
                    object_package_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::REPLICATIONIN => {
                    replication_inbound_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::REPLICATIONOUT => {
                    replication_outbound_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
            }
        }
        Ok(RuleEngine {
            root: root_expressions,
            obj: object_expressions,
            bundle: bundle_expressions,
            obj_package: object_package_expressions,
            repl_in: replication_inbound_expressions,
            repl_out: replication_outbound_expressions,
        })
    }

    pub fn has_root(&self) -> bool {
        !self.root.is_empty()
    }

    pub fn has_object(&self) -> bool {
        !self.obj.is_empty()
    }

    pub fn has_bundle(&self) -> bool {
        !self.bundle.is_empty()
    }

    pub fn has_object_package(&self) -> bool {
        !self.obj_package.is_empty()
    }

    #[allow(dead_code)]
    pub fn has_replication_in(&self) -> bool {
        !self.repl_in.is_empty()
    }

    #[allow(dead_code)]
    pub fn has_replication_out(&self) -> bool {
        !self.repl_out.is_empty()
    }

    #[tracing::instrument(level = "trace", skip(self, ctx))]
    pub fn evaluate_root(&self, ctx: RootRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = Value::resolve_all(&self.root, &context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::List(b) => Ok(b.iter().all(|x| match x {
                Value::Bool(b) => *b,
                _ => false,
            })),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }

    pub fn evaluate_object(&self, ctx: ObjectRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = Value::resolve_all(&self.obj, &context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::List(b) => {
                let result = b.iter().all(|x| match x {
                    Value::Bool(b) => *b,
                    _ => false,
                });
                Ok(result)
            }
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }

    pub fn evaluate_package(&self, ctx: PackageObjectRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = Value::resolve_all(&self.obj_package, &context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::List(b) => Ok(b.iter().all(|x| match x {
                Value::Bool(b) => *b,
                _ => false,
            })),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }

    pub fn evaluate_bundle(&self, ctx: BundleRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = Value::resolve_all(&self.bundle, &context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::List(b) => Ok(b.iter().all(|x| match x {
                Value::Bool(b) => *b,
                _ => false,
            })),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }

    pub fn _evaluate_incoming_replication(
        &self,
        ctx: ReplicationIncomingRuleInput,
    ) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = Value::resolve_all(&self.repl_in, &context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::List(b) => Ok(b.iter().all(|x| match x {
                Value::Bool(b) => *b,
                _ => false,
            })),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }

    pub fn _evaluate_outgoing_replication(
        &self,
        ctx: ReplicationOutgoingRuleInput,
    ) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = Value::resolve_all(&self.repl_out, &context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::List(b) => Ok(b.iter().all(|x| match x {
                Value::Bool(b) => *b,
                _ => false,
            })),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }
}
