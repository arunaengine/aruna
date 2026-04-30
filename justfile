local:
	bash scripts/local_deploy.sh

local-new:
	bash scripts/local_deploy.sh --new

local-cluster:
	bash scripts/local_cluster_deploy.sh

local-cluster-oidc:
	bash scripts/local_cluster_deploy.sh --with-keycloak
