local:
	bash scripts/local_deploy.sh

local-new:
	bash scripts/local_deploy.sh --new

local-cluster nodes="3":
	bash scripts/local_cluster_deploy.sh --node-count {{nodes}}

local-cluster-oidc nodes="3":
	bash scripts/local_cluster_deploy.sh --with-keycloak --node-count {{nodes}}
