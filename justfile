# Single-node compose stack with Keycloak; prints the REST, Swagger and Keycloak urls plus ADMIN_TOKEN.
local:
	bash scripts/local_deploy.sh

# Same compose stack from a wiped state directory; prints the same urls and a fresh ADMIN_TOKEN.
local-new:
	bash scripts/local_deploy.sh --new

# Realm of N local nodes without OIDC; prints per-node API, S3 and ops urls plus the admin credentials.
local-cluster nodes="3":
	bash scripts/local_cluster_deploy.sh --node-count {{nodes}}

# Realm of N local nodes with Keycloak; adds the OIDC issuer and the test logins to the printed summary.
local-cluster-oidc nodes="3":
	bash scripts/local_cluster_deploy.sh --with-keycloak --node-count {{nodes}}

# Realm of N local nodes with Keycloak and the portal on its own port per node; prints every url and login.
preview portal_dir=env_var_or_default("ARUNA_TEST_DEPLOY_PORTAL_DIR", "") nodes="3":
	bash scripts/local_cluster_deploy.sh --with-keycloak --node-count {{nodes}} --auto-portal-dir --portal-dir "{{portal_dir}}"

# Same without Keycloak, so the portal runs in guest mode; prints every url and the admin credentials.
preview-no-oidc portal_dir=env_var_or_default("ARUNA_TEST_DEPLOY_PORTAL_DIR", "") nodes="3":
	bash scripts/local_cluster_deploy.sh --node-count {{nodes}} --auto-portal-dir --portal-dir "{{portal_dir}}"

# Stops whatever a cluster recipe left running: the deploy script, every node by pid file, then Keycloak.
stop:
	bash scripts/local_cluster_stop.sh
