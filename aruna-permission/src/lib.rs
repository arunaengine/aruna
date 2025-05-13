use aruna_storage::storage::store::Store;
use async_trait::async_trait;
use casbin::{Adapter, CoreApi, DefaultModel, MgmtApi, Model, RbacApi};
use casbin::{Error, Filter};
use error::ArunaPermissionHandlerError;
use serde::{Deserialize, Serialize};

mod error;

/// The ABAC model configuration as a static string
pub static MODEL_CONF: &str = r#"
# Request definition
[request_definition]
r = sub, obj, act

# Policy definition
[policy_definition]
p = sub, obj, act, eft

# Role definition
[role_definition]
g = _, _

# Policy effect
[policy_effect]
e = some(where (p.eft == allow)) && !some(where (p.eft == deny))

# Matchers
[matchers]
m = g(r.sub, p.sub) || keyMatch2(r.sub, p.sub) && keyMatch2(r.obj, p.obj) && r.act == p.act
"#;

/// The Rule struct that bundles the policy rule components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub sec: String,
    pub ptype: String,
    pub rule: Vec<String>,
}

impl Rule {
    pub fn new(sec: &str, ptype: &str, rule: Vec<String>) -> Self {
        Self {
            sec: sec.to_owned(),
            ptype: ptype.to_owned(),
            rule,
        }
    }

    /// Generate a unique hash key for this rule
    pub fn hash_key(&self) -> [u8; 32] {
        let key_string = format!("{}:{}:{}", self.sec, self.ptype, self.rule.join(":"));
        let mut hasher = blake3::Hasher::new();
        hasher.update(key_string.as_bytes());
        hasher.finalize().into()
    }
}

/// The Casbin adapter that uses the generic Store trait
pub struct StoreAdapter<S: for<'a> Store<'a>> {
    store: S,
    db_name: &'static str,
    is_filtered: bool,
}

impl<S: for<'a> Store<'a>> StoreAdapter<S> {
    pub fn new(store: S, db_name: &'static str) -> Self {
        Self {
            store,
            db_name,
            is_filtered: false,
        }
    }

    /// Internal helper to convert a Rule to bytes for storage using postcard
    fn rule_to_bytes(&self, rule: &Rule) -> Result<Vec<u8>, ArunaPermissionHandlerError> {
        postcard::to_allocvec(rule).map_err(ArunaPermissionHandlerError::from)
    }

    /// Internal helper to convert bytes from storage to Rule using postcard
    fn bytes_to_rule(&self, bytes: &[u8]) -> Result<Rule, ArunaPermissionHandlerError> {
        postcard::from_bytes(bytes).map_err(ArunaPermissionHandlerError::from)
    }

    /// Load all rules from the store
    async fn load_rules(&self) -> Result<Vec<Rule>, ArunaPermissionHandlerError> {
        let txn = self.store.create_txn(false)?;
        let iter = self.store.iter_db(&txn, self.db_name)?;

        let mut rules = Vec::new();
        for (_, value) in iter {
            let rule = self.bytes_to_rule(&value)?;
            rules.push(rule);
        }

        self.store.commit(txn)?;
        Ok(rules)
    }

    /// Save a single rule to the store
    async fn save_rule(&self, rule: &Rule) -> Result<(), ArunaPermissionHandlerError> {
        let mut txn = self.store.create_txn(true)?;
        let key = rule.hash_key();
        let value = self.rule_to_bytes(rule)?;

        self.store.put(&mut txn, self.db_name, &key, &value)?;
        self.store.commit(txn)?;

        Ok(())
    }

    /// Remove a single rule from the store
    async fn remove_rule(&self, rule: &Rule) -> Result<(), ArunaPermissionHandlerError> {
        let mut txn = self.store.create_txn(true)?;
        let key = rule.hash_key();

        self.store.remove(&mut txn, self.db_name, &key)?;
        self.store.commit(txn)?;

        Ok(())
    }

    /// Clear all rules from the store
    async fn clear_rules(&self) -> Result<(), ArunaPermissionHandlerError> {
        let txn = self.store.create_txn(false)?;
        let iter = self.store.iter_db(&txn, self.db_name)?;

        let keys: Vec<Vec<u8>> = iter.map(|(k, _)| k.to_vec()).collect();
        self.store.commit(txn)?;

        let mut txn = self.store.create_txn(true)?;
        for key in keys {
            self.store.remove(&mut txn, self.db_name, &key)?;
        }
        self.store.commit(txn)?;

        Ok(())
    }
}

#[async_trait]
impl<S: for<'a> Store<'a> + Send + Sync> casbin::Adapter for StoreAdapter<S> {
    async fn load_policy(&mut self, m: &mut dyn Model) -> casbin::Result<()> {
        let rules = self.load_rules().await?;

        for rule in rules {
            let sec = rule.sec.as_str();
            let ptype = rule.ptype.as_str();

            if let Some(ref mut ast_map) = m.get_mut_model().get_mut(sec) {
                if let Some(ref mut ast) = ast_map.get_mut(ptype) {
                    let mut policy = Vec::new();
                    policy.push(ptype.to_owned());
                    policy.extend(rule.rule.clone());

                    ast.get_mut_policy().insert(policy);
                }
            }
        }

        Ok(())
    }

    async fn load_filtered_policy<'f>(
        &mut self,
        m: &mut dyn Model,
        f: Filter<'f>,
    ) -> casbin::Result<()> {
        let rules = self.load_rules().await?;
        self.is_filtered = false; // Start with false, will be updated if any filtering happens

        for rule in rules {
            let sec = rule.sec.as_str();
            let ptype = rule.ptype.as_str();

            // Skip rules that don't match the filter
            let mut should_add = true;

            if !f.p.is_empty() && sec == "p" {
                if !f.p.contains(&ptype) {
                    should_add = false;
                } else {
                    for (idx, value) in f.p.iter().enumerate() {
                        if !value.is_empty() && idx < rule.rule.len() && &rule.rule[idx] != value {
                            should_add = false;
                            break;
                        }
                    }
                }
                // If we're filtering, update the filtered flag
                if !should_add {
                    self.is_filtered = true;
                    continue;
                }
            }

            if !f.g.is_empty() && sec == "g" {
                if !f.g.contains(&ptype) {
                    should_add = false;
                } else {
                    for (idx, value) in f.g.iter().enumerate() {
                        if !value.is_empty() && idx < rule.rule.len() && &rule.rule[idx] != value {
                            should_add = false;
                            break;
                        }
                    }
                }
                // If we're filtering, update the filtered flag
                if !should_add {
                    self.is_filtered = true;
                    continue;
                }
            }

            // Add the rule to the model
            if should_add {
                if let Some(ref mut ast_map) = m.get_mut_model().get_mut(sec) {
                    if let Some(ref mut ast) = ast_map.get_mut(ptype) {
                        let mut policy = Vec::new();
                        policy.push(ptype.to_owned());
                        policy.extend(rule.rule.clone());

                        ast.get_mut_policy().insert(policy);
                    }
                }
            }
        }

        Ok(())
    }

    fn is_filtered(&self) -> bool {
        self.is_filtered
    }

    async fn save_policy(&mut self, m: &mut dyn Model) -> casbin::Result<()> {
        // Clear the database first
        self.clear_rules().await?;

        // Save each policy rule to the store
        for (sec, ast_map) in m.get_model() {
            for (ptype, ast) in ast_map {
                // Following the in-memory adapter pattern
                for policy in ast.get_policy() {
                    // The first element is always the policy type, which we already have
                    let rule_tokens = if policy.len() > 1 {
                        policy[1..].to_vec()
                    } else {
                        Vec::new()
                    };

                    let rule = Rule::new(sec, ptype, rule_tokens);
                    self.save_rule(&rule).await?;
                }
            }
        }

        Ok(())
    }

    async fn add_policy(
        &mut self,
        sec: &str,
        ptype: &str,
        rule: Vec<String>,
    ) -> casbin::Result<bool> {
        let rule = Rule::new(sec, ptype, rule);
        self.save_rule(&rule).await?;
        Ok(true)
    }

    async fn add_policies(
        &mut self,
        sec: &str,
        ptype: &str,
        rules: Vec<Vec<String>>,
    ) -> casbin::Result<bool> {
        for rule_tokens in rules {
            let rule = Rule::new(sec, ptype, rule_tokens);
            self.save_rule(&rule).await?;
        }
        Ok(true)
    }

    async fn remove_policy(
        &mut self,
        sec: &str,
        ptype: &str,
        rule: Vec<String>,
    ) -> casbin::Result<bool> {
        let rule = Rule::new(sec, ptype, rule);
        self.remove_rule(&rule).await?;
        Ok(true)
    }

    async fn remove_policies(
        &mut self,
        sec: &str,
        ptype: &str,
        rules: Vec<Vec<String>>,
    ) -> casbin::Result<bool> {
        for rule_tokens in rules {
            let rule = Rule::new(sec, ptype, rule_tokens);
            self.remove_rule(&rule).await?;
        }
        Ok(true)
    }

    async fn remove_filtered_policy(
        &mut self,
        sec: &str,
        ptype: &str,
        field_index: usize,
        field_values: Vec<String>,
    ) -> casbin::Result<bool> {
        // Load all rules
        let rules = self.load_rules().await?;
        let mut removed = false;

        for rule in rules {
            if rule.sec == sec && rule.ptype == ptype {
                let mut matched = true;

                for (i, field_value) in field_values.iter().enumerate() {
                    let field_idx = field_index + i;
                    if field_idx < rule.rule.len() {
                        if !field_value.is_empty() && rule.rule[field_idx] != *field_value {
                            matched = false;
                            break;
                        }
                    } else {
                        matched = false;
                        break;
                    }
                }

                if matched {
                    self.remove_rule(&rule).await?;
                    removed = true;
                }
            }
        }

        Ok(removed)
    }

    async fn clear_policy(&mut self) -> casbin::Result<()> {
        self.clear_rules().await.map_err(Into::into)
    }
}

/// The custom Enforcer struct that integrates the StoreAdapter
pub struct Enforcer {
    pub inner: casbin::Enforcer,
}

impl Enforcer {
    /// Create a new Enforcer with the provided store
    pub async fn new<S>(store: S, db_name: &'static str) -> casbin::Result<Self>
    where
        S: for<'a> Store<'a> + Send + Sync + 'static,
    {
        // Create the adapter
        let adapter = StoreAdapter::new(store, db_name);

        // Create the casbin enforcer
        let mut inner =
            casbin::Enforcer::new(DefaultModel::from_str(MODEL_CONF).await?, adapter).await?;

        // Load the policy
        inner.load_policy().await?;

        Ok(Self { inner })
    }

    /// Check if a request is permitted
    pub async fn enforce(&self, sub: &str, obj: &str, act: &str) -> casbin::Result<bool> {
        self.inner.enforce((sub, obj, act))
    }

    /// Add a policy rule
    pub async fn add_policy(
        &mut self,
        sub: &str,
        obj: &str,
        act: &str,
        eft: Option<&str>,
    ) -> casbin::Result<bool> {
        let mut rule = vec![sub.to_owned(), obj.to_owned(), act.to_owned()];
        if let Some(eft) = eft {
            rule.push(eft.to_owned());
        } else {
            rule.push("allow".to_owned());
        }
        self.inner.add_policy(rule).await
    }

    /// Remove a policy rule
    pub async fn remove_policy(&mut self, sub: &str, obj: &str, act: &str) -> casbin::Result<bool> {
        let rule = vec![sub.to_owned(), obj.to_owned(), act.to_owned()];
        self.inner.remove_policy(rule).await
    }

    /// Add a role assignment
    pub async fn add_group(&mut self, user: &str, role: &str) -> casbin::Result<bool> {
        self.inner
            .add_named_grouping_policy("g", vec![user.to_owned(), role.to_owned()])
            .await
    }

    /// Remove a role assignment
    pub async fn remove_group(&mut self, user: &str, role: &str) -> casbin::Result<bool> {
        self.inner
            .remove_named_grouping_policy("g", vec![user.to_owned(), role.to_owned()])
            .await
    }

    /// Get all policies
    pub async fn get_policies(&self) -> Vec<Vec<String>> {
        self.inner.get_policy()
    }

    /// Get all role assignments
    pub async fn get_groups(&self) -> Vec<Vec<String>> {
        self.inner.get_named_grouping_policy("g")
    }

    /// Save all policies to storage
    pub async fn save_policy(&mut self) -> casbin::Result<()> {
        self.inner.save_policy().await
    }

    /// Check if a user has a role
    pub async fn has_group(&self, user: &str, role: &str) -> bool {
        self.inner
            .has_grouping_named_policy("g", vec![user.to_owned(), role.to_owned()])
    }

    /// Get all roles that a user has
    pub async fn get_roles_for_user(&self, user: &str) -> Vec<String> {
        self.inner.get_implicit_roles_for_user(user, None)
    }

    /// Get all users that have a role
    pub async fn get_users_for_role(&self, role: &str) -> Vec<String> {
        self.inner.get_users_for_role(role, None)
    }

    /// Get all permissions for a user
    pub async fn get_permissions_for_user(&self, user: &str) -> Vec<Vec<String>> {
        self.inner.get_implicit_permissions_for_user(user, None)
    }

    /// Get all users that have a permission
    pub async fn get_users_for_permission(&self, obj: &str, act: &str) -> Vec<String> {
        let perm = vec![obj.to_owned(), act.to_owned()];
        self.inner.get_implicit_users_for_permission(perm).await
    }
}
