# Security policy

## Reporting a vulnerability

Please do **not** open a public issue for a security problem.

Report it through GitHub's private vulnerability reporting for this repository ("Security" tab → "Report a vulnerability"), or by email to **security@lokadb.com** with the subject "LokaDB security".

Include what you found, the version or commit, how to reproduce it, and what an attacker gains. We will answer within a few days. We will credit you in the fix unless you prefer not to be named.

## What is a vulnerability here

LokaDB's promise is narrow, and so is the scope: **the rows of the LokaDB tables of a moved organization are stored only in that organization's database**, and the proxy never serves one organization's rows to another. Examples that belong here:

- a statement that makes the proxy route to, read from or write into another organization's database or partition;
- a way for the application role (or any role the proxy admits) to change `lokadb.*` settings, policies or routing metadata through the proxy;
- a flaw in the tunnel: enrolment, certificate renewal or revocation, the relay admitting a peer it should not, an agent forwarding to an address other than its `-db`, the cloud window letting more than the move's role through;
- credentials of an organization database read by someone who should not (the control-plane storage, logs, the recording);
- a way to make the relay or the proxy execute or read something outside its job.

What is **not** a vulnerability, because it is documented: the application's own caches, queues, search indexes, files and logs; backups and replicas made before a move; a connection to the cloud database that bypasses the proxy; rows passing through the cloud Postgres on the `fdw` profile; LokaDB not being an authorization layer. See [docs/limits.md](docs/limits.md#what-lokadb-does-not-isolate).

## Supported versions

LokaDB is pre-release: only the current `main` receives fixes.
