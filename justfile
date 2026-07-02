local:
	bash scripts/local_deploy.sh

local-new:
	bash scripts/local_deploy.sh --new

local-cluster nodes="3":
	bash scripts/local_cluster_deploy.sh --node-count {{nodes}}

local-cluster-oidc nodes="3":
	bash scripts/local_cluster_deploy.sh --with-keycloak --node-count {{nodes}}

# 3-node realm + Keycloak, portal served from every node's REST port.
preview portal_dir="../tests/webtest/dist" nodes="3":
	bash scripts/local_cluster_deploy.sh --with-keycloak --node-count {{nodes}} --portal-dir {{portal_dir}}

# Same without Keycloak (portal in guest mode, no login).
preview-no-oidc portal_dir="../tests/webtest/dist" nodes="3":
	bash scripts/local_cluster_deploy.sh --node-count {{nodes}} --portal-dir {{portal_dir}}
