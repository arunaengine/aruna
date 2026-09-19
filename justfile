# Local recipes for linting, testing, checking and deploying, as CI runs them.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

# Style, formatting and Clippy, as CI runs them.
lint:
	python3 scripts/dev/check_style_tests.py
	python3 scripts/dev/check_style.py
	cargo +nightly-2026-09-14 fmt --all -- --check
	cargo +nightly-2026-09-14 clippy --workspace --all-targets --all-features --locked -- -D warnings

# Static structure, naming, and comment style check; no toolchain or build.
style:
	python3 scripts/dev/check_style_tests.py
	python3 scripts/dev/check_style.py

# Workspace tests and doctests, as the CI tests job runs them.
test:
	cargo nextest run --workspace --all-targets --all-features --locked --profile ci
	cargo test --workspace --all-features --locked --doc

# The fast editing loop: selects the aruna-core and aruna-operations lib test
# targets, still compiles their transitive dependencies, and executes only the
# audited no-I/O state-machine, pure, decision and reducer tests.
test-fast:
	cargo nextest run -p aruna-core -p aruna-operations --lib --locked --profile fast

# The same selection across every workspace target; compiles more than it runs.
test-fast-workspace:
	cargo nextest run --workspace --all-targets --all-features --locked --profile fast

# Compile every supported feature selection, as the CI feature checks run them.
check:
	cargo check --workspace --locked
	cargo check --workspace --all-targets --all-features --locked
	cargo check -p aruna-compute --all-targets --no-default-features --locked
	cargo check -p aruna-compute --all-targets --no-default-features --features docker --locked
	cargo check -p aruna-compute --all-targets --no-default-features --features apptainer --locked
	cargo check -p aruna-compute --all-targets --no-default-features --features kubernetes --locked
	cargo check -p aruna --all-targets --no-default-features --locked
	cargo check -p aruna --all-targets --no-default-features --features docker --locked
	cargo check -p aruna --all-targets --no-default-features --features apptainer --locked
	cargo check -p aruna --all-targets --no-default-features --features kubernetes --locked

# Single-node stack with Keycloak; prints service URLs and ADMIN_TOKEN.
local:
	bash scripts/local_deploy.sh

# Same compose stack from a wiped state directory; prints the same urls and a fresh ADMIN_TOKEN.
local-new:
	bash scripts/local_deploy.sh --new

# Local realm without OIDC; prints per-node service URLs and admin credentials.
local-cluster nodes="3":
	bash scripts/cluster_start.sh --node-count {{nodes}}

# Local realm with Keycloak; prints service URLs, OIDC issuer and test logins.
local-cluster-oidc nodes="3":
	bash scripts/cluster_start.sh --with-keycloak --node-count {{nodes}}

# Local realm with Keycloak and a portal per node; prints service URLs and logins.
preview portal_dir=env_var_or_default("ARUNA_TEST_DEPLOY_PORTAL_DIR", "") nodes="3":
	bash scripts/cluster_start.sh --with-keycloak --node-count "{{nodes}}" --auto-portal-dir --portal-dir "{{portal_dir}}"

# Same without Keycloak, so the portal runs in guest mode; prints every url and the admin credentials.
preview-no-oidc portal_dir=env_var_or_default("ARUNA_TEST_DEPLOY_PORTAL_DIR", "") nodes="3":
	bash scripts/cluster_start.sh --node-count "{{nodes}}" --auto-portal-dir --portal-dir "{{portal_dir}}"

# Stops the cluster script, its nodes identified by pid files, and Keycloak.
stop:
	bash scripts/cluster_stop.sh
