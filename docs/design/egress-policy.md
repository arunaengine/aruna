# Connector egress policy (SSRF protection)

Every server-side fetch — staging source connectors (HTTP/S3/WebDAV/FTP), OIDC
issuer discovery, portal artifact downloads, and onboarding bootstrap — is
default-deny. The `aruna-egress` crate filters DNS answers at connect time, so a
destination that resolves to a loopback, private, link-local, or metadata
address is refused even if a hostname rebinds between lookups.

## Built-in deny table

Denied by default (unwrapping IPv4-mapped and NAT64 IPv6 forms first):

- IPv4: `0.0.0.0/8`, `10/8`, `100.64/10`, `127/8`, `169.254/16` (incl.
  `169.254.169.254`), `172.16/12`, `192.0.0.0/24`, `192.168/16`, `198.18/15`,
  `224/4`, `240/4`, `255.255.255.255/32`.
- IPv6: `::/128`, `::1/128`, `fe80::/10`, `fc00::/7` (incl. `fd00:ec2::254`),
  `ff00::/8`.

Schemes are restricted per connector kind (HTTP/S3/WebDAV → `http`/`https`;
FTP → `ftp`), and an explicit port `0` is rejected.

## Allowlist

Internal deployments that legitimately fetch from private ranges **must
configure the realm egress allowlist**. It is realm config (Class-1, replicated
to every node) and edited by a realm config admin:

- `GET /info/realm/egress` — read the current allowlist (realm member).
- `PUT /info/realm/egress` — replace it (management node + WRITE on
  `/{realm_id}/admin/config`).

Each rule names a `host` (an exact hostname or a CIDR) and may narrow `ports`
and `schemes`. A hostname rule exempts the resolved addresses of that name; a
CIDR rule exempts the addresses it contains.

## Notes and limitations

- **Mixed DNS answers hard-fail.** A hostname resolving to both a permitted and
  a denied address is refused unless allowlisted. Split-horizon deployments must
  allowlist the affected hosts.
- **FTP is allowlist-only.** The control connection is pinned to one validated
  IP literal, but the PASV data channel stays server-controlled, so an FTP
  endpoint is refused unless an allowlist rule covers it.
- **Pre-realm-config fetches** (onboarding bootstrap, portal download) run before
  any realm config exists. They honor the built-in deny table plus an
  `ARUNA_EGRESS_ALLOW` environment escape hatch (comma-separated hosts/CIDRs).
  This env allowlist never merges into the realm egress policy.
- **OIDC issuers** are subject to the built-in deny table only (no allowlist);
  the deny table still blocks rebinding and redirect tricks.
