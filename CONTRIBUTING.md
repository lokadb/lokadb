# Contributing

Issues and pull requests are welcome. For a larger change, open an issue first so we can agree on the approach. By contributing you agree that your contribution is licensed under the Apache License 2.0 (section 5 of the license).

## Building and testing

Go code is built and tested in Docker (`golang:1.25`), so everyone gets the same toolchain and C compiler (the SQL parser needs cgo). From the repository root:

```bash
docker run --rm -v "$PWD/proxy:/src" -w /src golang:1.25 go vet ./...
docker run --rm -v "$PWD/proxy:/src" -w /src golang:1.25 go test ./...
docker run --rm -v "$PWD/proxy:/src" -w /src golang:1.25 go test -race ./...
docker build -t lokadb proxy      # the image with the lokadb binary
```

Add `-v lokadb-gomod:/go/pkg/mod` to keep the module cache between runs.

Unit tests need no database. Many checks need real Postgres and live in the end-to-end stand and in the defect checks, both of which run under Docker Compose.

## End-to-end stand and defect checks

```bash
bash e2e/run.sh                                  # three applications, the tunnel, check, move-org, move-back (about 20 min)
bash e2e/defects/defects.sh                      # one check per past defect, on fresh databases
DEFECTS_ONLY=<name>,<name> bash e2e/defects/defects.sh
DEFECTS_MODE=trigger bash e2e/defects/defects.sh # the same with moves by triggers
```

The stand keeps Postgres data in memory (tmpfs) and starts about ten containers: give Docker around 8 GB, and do not run two stands at once from one directory (`E2E_NET=232 COMPOSE_PROJECT_NAME=lokadb-e2e2` runs a second one from a second copy). See [docs/testing.md](docs/testing.md).

**A change to routing, moving, the tunnel or the schema conversion should come with a check in `e2e/defects` that fails before the change and passes after it.** That is how the defects fixed so far stay fixed.

## Style

- Go: `gofmt`, `go vet` clean. Comments say *why*, not what; match the density and idiom of the surrounding code. No new dependency without a reason in the pull request.
- SQL that LokaDB generates is written for Postgres 17 and 18; quote identifiers with the existing helpers, never by string concatenation.
- Log lines never contain values from data rows, passwords or tokens; SQL in logs has its literals replaced by `$n`.
- Documentation is English, in `docs/`; a user-visible change updates the page that describes it. Measured numbers go with their method, hardware and date in `research/bench/`, never into prose alone.
- Commit messages: a short summary line, then what changed and why.

## Reporting a security problem

Not through an issue: see [SECURITY.md](SECURITY.md). Other questions: hello@keepplain.com.
