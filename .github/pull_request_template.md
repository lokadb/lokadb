## What and why

## Checks

- [ ] `go vet ./...` and `go test ./...` pass (in Docker, `proxy/`)
- [ ] a change to routing, moving, the tunnel or the schema conversion comes with a check in `e2e/defects` that fails without it
- [ ] documentation updated for a user-visible change
- [ ] no values from data rows, passwords, tokens or addresses of real servers in code, logs, tests or documents
