use aruna_storage::storage::store::Store;
use casbin::MemoryAdapter;
use casbin::{CoreApi, DefaultModel, MgmtApi, RbacApi};

use crate::casbin_helper::{CasbinPolicy, CasbinRole};

pub const MODEL_CONF: &str = r#"
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act, eft

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow)) && !some(where (p.eft == deny))

[matchers]
m = g(r.sub, p.sub) && keyMatch2(r.obj, p.obj) && (r.act == p.act || (p.act == "write" && r.act == "read"))
"#;

pub const DBNAME: &str = "casbin";

/// The custom Enforcer struct that integrates the StoreAdapter
pub struct Enforcer {
    pub inner: casbin::Enforcer,
}

impl Enforcer {
    /// Create a new Enforcer with the provided store
    pub async fn new() -> casbin::Result<Self> {
        // Create the adapter
        let adapter = MemoryAdapter::default();

        // Create the casbin enforcer
        let mut inner =
            casbin::Enforcer::new(DefaultModel::from_str(MODEL_CONF).await?, adapter).await?;
        // inner.enable_log(true);

        // Load the policy
        inner.load_policy().await?;

        Ok(Self { inner })
    }

    /// Check if a request is permitted
    pub fn enforce(&self, sub: &str, obj: &str, act: &str) -> casbin::Result<bool> {
        self.inner.enforce((sub, obj, act))
    }

    /// Add a policy rule
    pub async fn add_policy(&mut self, policy: CasbinPolicy) -> casbin::Result<bool> {
        let policy = policy.commit();
        self.inner.add_policy(policy).await
    }

    pub async fn load_policy<'a, S>(
        &mut self,
        store: &'a S,
        txn: &'a <S as Store<'a>>::Txn,
    ) -> casbin::Result<()>
    where
        S: Store<'a> + 'static,
    {
        for (key, _) in store
            .iter_db(txn, DBNAME)
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?
        {
            let key = String::from_utf8(key.to_vec()).unwrap();
            if key.starts_with("p:") {
                let rule = key[2..]
                    .split(':')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                self.inner.add_policy(rule).await?;
            } else if key.starts_with("g:") {
                let rule = key[2..]
                    .split(':')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                self.inner.add_named_grouping_policy("g", rule).await?;
            }
        }
        self.inner.load_policy().await
    }

    /// Remove a policy rule
    pub async fn remove_policy(&mut self, policy: CasbinPolicy) -> casbin::Result<bool> {
        let rule = policy.commit();
        self.inner.remove_policy(rule).await
    }

    /// Add a role assignment
    pub async fn add_role(&mut self, role: CasbinRole) -> casbin::Result<bool> {
        let role = role.commit();
        self.inner.add_named_grouping_policy("g", role).await
    }

    /// Remove a role assignment
    pub async fn remove_role(&mut self, role: CasbinRole) -> casbin::Result<bool> {
        let role = role.commit();
        self.inner.remove_named_grouping_policy("g", role).await
    }

    /// Get all policies
    pub fn get_roles(&self) -> Vec<Vec<String>> {
        self.inner.get_grouping_policy()
    }

    /// Get all role assignments
    pub fn get_groups(&self) -> Vec<Vec<String>> {
        self.inner.get_named_grouping_policy("g")
    }

    /// Get all policies
    pub fn get_policies(&self) -> Vec<Vec<String>> {
        self.inner.get_policy()
    }

    /// Save all policies to storage
    pub async fn save_policy(&mut self) -> casbin::Result<()> {
        self.inner.save_policy().await
    }

    /// Check if a user has a role
    pub fn has_role(&self, user: &str, role: &str) -> bool {
        self.inner
            .has_grouping_named_policy("g", vec![user.to_owned(), role.to_owned()])
    }

    /// Get all roles that a user has
    pub fn get_roles_for_user(&self, user: &str) -> Vec<String> {
        self.inner.get_implicit_roles_for_user(user, None)
    }

    /// Get all users that have a role
    pub fn get_users_for_role(&self, group: &str) -> Vec<String> {
        self.inner.get_users_for_role(group, None)
    }

    /// Get all permissions for a user
    pub fn get_permissions_for_user(&self, user: &str) -> Vec<Vec<String>> {
        self.inner.get_implicit_permissions_for_user(user, None)
    }

    /// Get all users that have a permission
    pub async fn get_users_for_permission(&self, obj: &str, act: &str) -> Vec<String> {
        let perm = vec![obj.to_owned(), act.to_owned()];
        self.inner.get_implicit_users_for_permission(perm).await
    }
}
