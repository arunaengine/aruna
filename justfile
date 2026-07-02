local:
	bash scripts/local_deploy.sh

local-new:
	bash scripts/local_deploy.sh --new

local-cluster nodes="3":
	bash scripts/local_cluster_deploy.sh --node-count {{nodes}}

local-cluster-oidc nodes="3":
	bash scripts/local_cluster_deploy.sh --with-keycloak --node-count {{nodes}}

# 3-node realm + Keycloak, portal served from every node's REST port.
preview portal_dir=env_var_or_default("ARUNA_TEST_DEPLOY_PORTAL_DIR", "") nodes="3":
	bash scripts/local_cluster_deploy.sh --with-keycloak --node-count {{nodes}} --require-portal-dir --portal-dir "{{portal_dir}}"

# Same without Keycloak (portal in guest mode, no login).
preview-no-oidc portal_dir=env_var_or_default("ARUNA_TEST_DEPLOY_PORTAL_DIR", "") nodes="3":
	bash scripts/local_cluster_deploy.sh --node-count {{nodes}} --require-portal-dir --portal-dir "{{portal_dir}}"
