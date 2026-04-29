compose:
	bash scripts/bootstrap_compose.sh

compose-new:
	bash scripts/bootstrap_compose.sh --new

test-deploy:
	bash scripts/test_deploy.sh

test-deploy-oidc:
	bash scripts/test_deploy.sh --with-keycloak
