# The tunnel: relay and agent

The cloud never connects into the customer's network. An **agent** inside that network opens the only connection, outbound, to a **relay** in the cloud, and every cloud access to the customer database goes through it: `postgres_fdw`, the proxy's direct path and DDL replay, `move-org`.

| | Where it runs | What it does |
|---|---|---|
| `lokadb agent` | in the organization's network, next to its Postgres | Creates its own key, enrols with a one-time token, renews its certificate. Opens an **outbound** TLS connection to the relay and keeps it (keepalive, reconnect). Forwards streams from the tunnel **only** to the address in `-db`. Publishes no ports. |
| `lokadb relay` | in the cloud network | Issues and renews agent certificates. Admits only certificates issued by enrolment and not revoked. Knows the organization from the certificate (`CN=org-<N>`). For each organization, listens on a port inside the cloud; a connection to it becomes a stream to that organization's agent. Only addresses in `org_allow` may connect. |
| `lokadb token` / `revoke` / `agents` / `org-endpoint` | cloud administrator | Issue a one-time token, revoke an organization's certificates, list issued certificates, manage the organization's port on the relay. Stored in the registry (the control-plane database). |
| `lokadb certs` | once | Creates a demo CA and relay certificate (does not overwrite an existing CA without `-force`). |

Inside the tunnel the Postgres protocol passes unchanged: the organization's database checks the password itself. If the agent is not connected, the relay answers with a real Postgres error; if the agent cannot reach the database, the agent does. The application sees the reason:

```
ERROR:  could not connect to server "lokadb_org1"
DETAIL:  ... FATAL:  lokadb relay: the agent of organization 1 is not connected
```

## Enrolment, renewal and revocation

1. **Token.** `lokadb token -cp "$CP_DSN" -org 1 -ttl 15m` prints the token once; the registry keeps only its sha256, the organization and the expiry.
2. **Install.** The agent needs three things: the CA certificate (to verify the relay), the token in a file (`-token-file`, not on the command line), and a state directory (`-state`).
3. **Enrol.** The agent creates a P-256 key and sends only a CSR and the token. The relay validates the CSR and that it serves the token's organization (neither failure burns the token), then consumes the token atomically and signs a certificate for `cert_ttl`. **The agent's key never leaves its machine.**
4. **Renew.** At two thirds of the lifetime the agent creates a new key and gets a new certificate over mutual TLS. Revoked, expired or non-enrolled certificates cannot renew. A live session is not cut when the certificate expires; the next connection uses the new one. The previous certificate stays valid until the new one is used, so a lost renewal response does not lock the agent out. A certificate that expired at most `renew_expired` ago (24 h by default) can still renew — the agent recovers on its own after the relay or registry was down.
5. **Restart.** The agent reuses the certificate from `-state` (one file, mode 0600, replaced atomically); no new token.
6. **Revoke.** `lokadb revoke -cp "$CP_DSN" -org 1` revokes all certificates of the organization and its unused tokens. The relay checks live sessions against the registry every two seconds and cuts revoked ones. `-serial S` revokes one agent with its whole renewal chain. Only a new token brings the organization back.

Failed enrolment and renewal attempts are limited per client address (`enroll_fail_per_min`, 10). Each registry call made for a connection is limited to 5 s.

## CA key in a separate issuing service

`lokadb issuer -config issuer.json` is the only process holding the CA key. A relay with `"issuer_url"` holds no CA key: it sends validated CSRs to the issuer over mutual TLS. The issuer signs only agent client certificates, only for relays listed in `relays`, and logs every issue; the relay checks the answer, and burns the token only after a successful signature. A compromised relay can request agent certificates while it is a relay, but cannot take the CA key, issue a server certificate, or hide an issue from the issuer's log. Signing goes through `crypto.Signer`, which is where a KMS or HSM fits (no ready integration yet).

## Rotating the CA without restarts

Relays and the issuer re-read their certificate, CA bundle and CA key when the files change; the agent re-reads its CA bundle on every connection, and also trusts the bundle the relay sends with each certificate. The first CA of the bundle signs.

1. `ca.crt` = old CA + new CA on relays and the issuer; wait until every agent has renewed (`cert_ttl`).
2. Put a relay certificate from the new CA in `relay.crt` / `relay.key`.
3. `ca.crt` = new CA first + old, `ca.key` = the new key: new agent certificates come from the new CA.
4. After every agent has renewed, drop the old CA.

The relay logs, daily and at start, when its certificate or a CA of its bundle expires in less than 30 days.

## Several relays

Relays keep no state: everything is in the registry. Several relays with a shared registry and one CA (or one issuer) listen on the same organization ports behind a TCP load balancer; the organization's FDW server points at the balancer. The agent keeps a session **with every relay** (`-relay a:7443,b:7443`, or one name resolving to every relay, re-resolved every 30 s), so any relay reaches the organization and there is no inter-relay protocol. Stopping one relay cuts only the connections that went through it.

```yaml
# Kubernetes: a Service in front of the relay pods; readiness on /healthz
ports: [{name: org1, port: 15001}]
readinessProbe: {httpGet: {path: /healthz, port: 7446}, periodSeconds: 2}
terminationGracePeriodSeconds: 90   # more than drain_delay + drain_timeout
```

```
# HAProxy or a cloud NLB: TCP, health check over HTTP on a separate port
backend org1
  mode tcp
  option httpchk GET /healthz
  server relay-a relay-a:15001 check port 7446 inter 2s
  server relay-b relay-b:15001 check port 7446 inter 2s
```

**Graceful stop.** On `SIGTERM` the relay answers 503 on `/healthz` at once, closes its ports after `drain_delay` (default `0`), asks agents to open no new streams, and lets open streams finish until `drain_timeout` (default `5s`). With several relays raise both (for example `5s` and `1m`). Connection pools (`postgres_fdw`, the proxy's pools) keep connections open, so with pools the drain usually lasts the whole `drain_timeout` and the remaining connections are then cut: an upgrade without interruption needs several relays behind a balancer. Docker kills a container after 10 s by default — set `stop_grace_period` higher.

The relay starts without the registry (on the organizations of its `relay.json`) and connects when the registry comes back. While the registry is down, **revocation does not reach live sessions**; `registry_grace` admits only agents whose certificate the registry confirmed within that period.

## Several agents per organization

An organization can run several agents (for example on two hosts), each enrolled with its own token. The relay gives a new stream to the agent with the fewest open streams; losing one agent cuts only its streams. `-sessions N` (1–8) keeps N TCP connections per relay so that packet loss on one does not delay the others. `max_agents_per_org` (4) limits agents per organization. All agents of an organization must lead to the same database — the relay cannot check that.

## A new organization without restarting the relay

The relay rebuilds its list of organizations every two seconds from `orgs` in its `relay.json` and from the registry:

```bash
lokadb org-endpoint -cp "$CP" -org 2 -listen :15002   # the relay listens on :15002 within 2 s
lokadb token -cp "$CP" -org 2                          # token for organization 2's agent
lokadb org-endpoint -cp "$CP" -org 2 -remove           # port closed, agent disconnected
```

On conflict the file wins. An unreadable file or an unreachable registry keeps the last good list. A token of an organization the relay does not serve yet is not burned; an agent started right after `org-endpoint` waits up to 30 s for the port.

## The way to the cloud: subscription during a move

A logical move needs the customer database to subscribe to a cloud publication. That connection also goes through the tunnel (agent `-cloud-listen` → relay → `cloud` in `relay.json`), so the cloud Postgres is never exposed. It is closed unless a move is running: the relay admits it only while `move-org` holds a window open in the registry; only the move's temporary replication role, to that database, in replication mode passes; the agent accepts it only from addresses in `-cloud-allow` (by default its database host, loopback and the agent host's own interface addresses). When the window closes, open streams to the cloud are closed within two seconds.

## TLS end to end

The relay and agent pass bytes as they are, so TLS can run from `postgres_fdw`, the proxy or `move-org` all the way to the organization's database: turn on `ssl` there and use `sslmode=require` or stricter (`verify-full` with `sslrootcert`) in the organization's DSNs. `move-org` copies the TLS settings into the options of the organization's FDW server (`postgres_fdw` takes TLS only from there); file paths are read by the cloud Postgres, so the files must be on its server.

To make sure no one forgets, require TLS on the relay — for all organizations (`"org_tls": "require"`) or one (`org-endpoint -org N -tls require`). The relay then refuses a connection that does not start with `SSLRequest`, does not let a client continue in clear text if the database refuses TLS, and still passes `CancelRequest`.

Between the application and the proxy, turn on `tls` in `lokadb.json`: the proxy receives the application's password in clear text to pass it to Postgres. See [Configuration](configuration.md#proxy-lokadbjson).

## Limits

| Setting | Default | Limits |
|---|---|---|
| `max_org_conns` | 1000 | connections per organization endpoint |
| `max_streams` | 2048 | connections through the relay in total; memory up to about `max_streams × 4 MiB` (the multiplexer window, `LOKADB_MUX_WINDOW_KB`) — size it to the machine |
| `max_handshakes` | 256 | agents in TLS handshake |
| `max_enroll_conns` | 256 | connections to the enrolment port |
| `max_cloud_streams` | 64 | connections to the cloud per agent session |
| agent `-max-conns` | 200 | connections to the organization's database |

Above a limit the connection is refused with `08006` and the setting named in the log. A negative value removes a limit.

The `org_allow` list is required: without it the relay does not start. List the cloud addresses that reach organizations — the proxy, the cloud Postgres, the machines that run `check` and `move-org`. Otherwise anyone who can reach an organization port on the relay would get a connection to that organization's database through its agent.

## Troubleshooting

| What you see | Cause | What to do |
|---|---|---|
| The application gets `lokadb relay: the agent of organization N is not connected` | no agent of that organization has a session with the relay | on the agent's host: is it running, and what does its log say (`tunnel up` or the reason it cannot connect)? Its `GET /status` (`-status-listen`) shows `state` and, per relay, `last_error`. On the relay side: `lokadb agents -cp "$CP_DSN"` lists the certificates, `GET /sessions` on `health_listen` the live sessions |
| `lokadb relay: organization N is not available until the relay reads the registry` | the relay has not read the control plane yet | check that the relay reaches the control plane database (`control_plane` in `relay.json`) |
| The relay logs `organization endpoint: connection refused, not in org_allow` | the connecting machine's address is not listed | add it to `org_allow` (the proxy, the cloud Postgres, every machine that runs `check` or `move-org`) and restart the relay |
| Enrolment answers 429 `too many failed attempts` | more than `enroll_fail_per_min` (10) failed attempts from one address | wait a minute; fix the cause (a wrong token or CA) before retrying |
| The agent logs that the token was used or expired | a token works once, for its `-ttl` (15 minutes by default) | `lokadb token -cp "$CP_DSN" -org N` makes a new one |
| The agent's `state` is `revoked` | `lokadb revoke` was run for the organization | only a new token brings it back |
| The agent's `state` is `cert_expired`, or it logs `issue a new token` | the agent was down longer than the certificate's lifetime plus `renew_expired` | a new token ([restarts and upgrades](https://docs.lokadb.com/operations/restarts-and-upgrades/)) |
| A connection ends with `08006` and the log names a setting (`max_org_conns`, `max_streams`, agent `-max-conns`…) | a [limit](#limits) is reached | raise that setting, or find why connections pile up |
| `lokadb agent: connection from X to the cloud window refused; add it to -cloud-allow` | a logical move's subscription comes from an address the agent does not allow | add the address to the agent's `-cloud-allow` |

The agent's `state` is one of `connected`, `reconnecting`, `revoked`, `cert_expired`. Alerts worth having: the agent not `connected` for more than a few minutes; `cert_ends` (in the agent's status) less than a third of `cert_ttl` away, which means renewal is failing; the relay's `/healthz` not 200; the number of sessions on the relay's `/sessions` below the number of agents you run.
