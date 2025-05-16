use aruna_storage::storage::store::Store;
use casbin::MemoryAdapter;
use casbin::{CoreApi, DefaultModel, MgmtApi, RbacApi};

mod error;

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
pub struct Enforcer<'a, S: Store<'a> + Send + Sync + 'static> {
    pub inner: casbin::Enforcer,
    pub store: &'a S,
}

impl<'a, S: Store<'a>> Enforcer<'a, S> {
    /// Create a new Enforcer with the provided store
    pub async fn new(store: &'a S) -> casbin::Result<Self> {
        // Create the adapter
        let adapter = MemoryAdapter::default();

        // Create the casbin enforcer
        let mut inner =
            casbin::Enforcer::new(DefaultModel::from_str(MODEL_CONF).await?, adapter).await?;

        // Load the policy
        inner.load_policy().await?;

        Ok(Self { inner, store })
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
        txn: &mut <S as Store<'a>>::Txn,
    ) -> casbin::Result<bool> {
        let mut rule = vec![sub.to_owned(), obj.to_owned(), act.to_owned()];
        if let Some(eft) = eft {
            rule.push(eft.to_owned());
        } else {
            rule.push("allow".to_owned());
        }
        let key = format!("p:{}", rule.join(":"));
        let result = self.inner.add_policy(rule).await;

        self.store
            .put(txn, DBNAME, key.as_bytes(), &[])
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?;

        result
    }

    pub async fn load_policy(&mut self, txn: &<S as Store<'a>>::Txn) -> casbin::Result<()> {
        for (key, _) in self
            .store
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
    pub async fn remove_policy(
        &mut self,
        sub: &str,
        obj: &str,
        act: &str,
        eft: &str,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> casbin::Result<bool> {
        let rule = vec![
            sub.to_owned(),
            obj.to_owned(),
            act.to_owned(),
            eft.to_owned(),
        ];
        let key = format!("p:{}", rule.join(":"));
        let result = self.inner.remove_policy(rule).await;

        self.store
            .remove(txn, DBNAME, key.as_bytes())
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?;

        result
    }

    /// Add a role assignment
    pub async fn add_group(
        &mut self,
        user: &str,
        role: &str,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> casbin::Result<bool> {
        let key = format!("g:{}:{}", user, role);

        let result = self
            .inner
            .add_named_grouping_policy("g", vec![user.to_owned(), role.to_owned()])
            .await;

        self.store
            .put(txn, DBNAME, key.as_bytes(), &[])
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?;
        result
    }

    /// Remove a role assignment
    pub async fn remove_group(
        &mut self,
        user: &str,
        role: &str,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> casbin::Result<bool> {
        let key = format!("g:{}:{}", user, role);
        let result = self
            .inner
            .remove_named_grouping_policy("g", vec![user.to_owned(), role.to_owned()])
            .await;

        self.store
            .remove(txn, DBNAME, key.as_bytes())
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?;
        result
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
    pub async fn get_groups_for_user(&self, user: &str) -> Vec<String> {
        self.inner.get_implicit_roles_for_user(user, None)
    }

    /// Get all users that have a role
    pub async fn get_users_for_group(&self, group: &str) -> Vec<String> {
        self.inner.get_users_for_role(group, None)
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
