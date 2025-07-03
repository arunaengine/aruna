use crate::caching::structs::CachedRule;
use crate::database::dsls::rule_dsl::RuleBinding;
use crate::database::{crud::CrudDb, dsls::object_dsl::Object};
use crate::middlelayer::rule_request_types::{CreateRuleBinding, DeleteRuleBinding, UpdateRule};
use crate::notification::natsio_handler::{Action, Created, Deleted, ServerEvents, Updated};
use ahash::HashSet;
use anyhow::{anyhow, Ok, Result};
use cel_interpreter::Value;
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use tokio_postgres::Client;

use super::{db_handler::DatabaseHandler, rule_request_types::CreateRule};

impl DatabaseHandler {
    /// This function takes affected objects and the request object and collects all parent rules,
    /// then evaluates all rules and updates every affected child resource including the request
    /// object
    pub async fn evaluate_and_update_rules(
        &self,
        affected: &Vec<DieselUlid>,
        origin: &DieselUlid,
        transaction_client: &Client,
    ) -> Result<()> {
        // 1. Collect additional bindings
        let parents = Object::fetch_parents_by_id(origin, transaction_client).await?;
        let mut children = Object::fetch_subresources_by_id(origin, transaction_client).await?;
        children.insert(0, *origin);
        let mut cascading = HashSet::default();
        for parent in parents {
            if let Some(bindings) = self.cache.get_rule_bindings(&parent) {
                for binding in bindings.iter().filter(|b| b.cascading) {
                    cascading.insert(binding.clone());
                }
            }
        }
        // 2. Evaluate rules
        self.evaluate_rules(affected, transaction_client).await?;
        // 3. Evaluate additional rules
        self.evaluate_additional_rules(&children, &cascading, transaction_client)
            .await?;
        // 3. Update bindings
        let mut inherited_rules = HashSet::default(); // must be unique
        for child in children {
            for binding in cascading.clone().into_iter() {
                let mut new = RuleBinding {
                    rule_id: binding.rule_id,
                    origin_id: binding.origin_id,
                    object_id: child,
                    cascading: binding.cascading,
                };
                if inherited_rules.insert(new.clone()) {
                    new.create(transaction_client).await?;
                    self.cache.insert_rule_binding(vec![child], new.clone());
                }
            }
        }
        for binding in inherited_rules {
            if let Err(err) = self
                .natsio_handler
                .register_server_event(ServerEvents::CACHEUPDATE(Action::Updated(
                    Updated::RuleBinding {
                        rule_id: binding.rule_id,
                        origin_id: binding.origin_id,
                        resource_id: binding.object_id,
                    },
                )))
                .await
            {
                log::error!("{err}");
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }
        Ok(())
    }

    /// This function evaluates rules for a vec of object_ids
    pub async fn evaluate_rules(
        &self,
        affected: &Vec<DieselUlid>,
        transaction_client: &Client,
    ) -> Result<()> {
        // Policy evaluation:
        for id in affected {
            if let Some(bindings) = self.cache.get_rule_bindings(id) {
                for binding in bindings.clone().iter() {
                    self.evaluate_rule(&binding.rule_id, id, transaction_client)
                        .await?
                }
            }
        }
        Ok(())
    }

    pub async fn create_rule(
        &self,
        request: CreateRule,
        user_id: DieselUlid,
    ) -> Result<DieselUlid> {
        let client = self.database.get_client().await?;
        let mut rule = request.build_rule(user_id)?;
        let id = rule.rule.id;
        rule.rule.create(&client).await?;
        self.cache.insert_rule(&id, rule.clone());
        if let Err(err) = self
            .natsio_handler
            .register_server_event(ServerEvents::CACHEUPDATE(Action::Created(Created::Rule(
                id,
            ))))
            .await
        {
            log::error!("{err}");
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok(id)
    }

    pub async fn update_rule(
        &self,
        request: UpdateRule,
        rule: Arc<CachedRule>,
    ) -> Result<CachedRule> {
        let client = self.database.get_client().await?;
        let updated = request.merge(&rule)?;
        updated.rule.update(&client).await?;
        self.cache.insert_rule(&updated.rule.id, updated.clone());
        // TODO: Update rule event
        if let Err(err) = self
            .natsio_handler
            .register_server_event(ServerEvents::CACHEUPDATE(Action::Updated(Updated::Rule(
                updated.rule.id,
            ))))
            .await
        {
            log::error!("{err}");
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok(updated)
    }

    pub async fn delete_rule(&self, rule: &CachedRule) -> Result<()> {
        let client = self.database.get_client().await?;
        rule.rule.delete(&client).await?;
        self.cache.delete_rule(&rule.rule.id);
        if let Err(err) = self
            .natsio_handler
            .register_server_event(ServerEvents::CACHEUPDATE(Action::Deleted(Deleted::Rule(
                rule.rule.id,
            ))))
            .await
        {
            log::error!("{err}");
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok(())
    }

    pub async fn create_rule_binding(&self, request: CreateRuleBinding) -> Result<()> {
        let client = self.database.get_client().await?;
        let mut binding = request.get_binding()?;
        binding.create(&client).await?;
        let resource_ids = if request.0.cascading {
            let mut ids = vec![binding.origin_id];
            ids.append(&mut self.cache.get_subresources(&binding.origin_id)?);
            ids
        } else {
            vec![binding.origin_id]
        };
        self.cache
            .insert_rule_binding(resource_ids, binding.clone());
        if let Err(err) = self
            .natsio_handler
            .register_server_event(ServerEvents::CACHEUPDATE(Action::Created(
                Created::RuleBinding {
                    rule_id: binding.rule_id,
                    origin_id: binding.origin_id,
                    resource_id: binding.object_id,
                },
            )))
            .await
        {
            log::error!("{err}");
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok(())
    }
    pub async fn delete_rule_binding(&self, request: DeleteRuleBinding) -> Result<()> {
        let client = self.database.get_client().await?;
        let (resource_id, rule_id) = request.get_ids()?;
        RuleBinding::delete_by(rule_id, resource_id, &client).await?;
        self.cache.remove_rule_bindings(resource_id, rule_id);
        if let Err(err) = self
            .natsio_handler
            .register_server_event(ServerEvents::CACHEUPDATE(Action::Deleted(
                Deleted::RuleBinding {
                    rule_id,
                    resource_id,
                },
            )))
            .await
        {
            log::error!("{err}");
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok(())
    }
    async fn evaluate_additional_rules(
        &self,
        children: &Vec<DieselUlid>,
        rules: &HashSet<RuleBinding>,
        transaction_client: &Client,
    ) -> Result<()> {
        for child in children {
            for rule in rules.iter().map(|r| r.rule_id) {
                self.evaluate_rule(&rule, child, transaction_client).await?
            }
        }
        Ok(())
    }
    async fn evaluate_rule(
        &self,
        rule_id: &DieselUlid,
        object_id: &DieselUlid,
        transaction_client: &Client,
    ) -> Result<()> {
        if let Some(rule) = self.cache.get_rule(rule_id) {
            let current_state =
                Object::get_object_with_relations(object_id, transaction_client).await?;

            let mut ctx = cel_interpreter::Context::default();
            ctx.add_variable("object", current_state.clone())
                .map_err(|e| anyhow!(e.to_string()))?;
            let value = Value::resolve(&rule.compiled, &ctx)
                .map_err(|e| anyhow!(format!("Policy evaluation error: {}", e.to_string())))?;
            if value != Value::Bool(true) {
                return Err(anyhow!(format!("Policy {} evaluated false", rule.rule.id)));
            }
        } else {
            return Err(anyhow!("Rules and Bindings are out of sync"));
        }
        Ok(())
    }
}
