# Workflows

`github.com/looprig/workflows` is the storage-neutral bridge between the
[Flow](https://github.com/looprig/flow) workflow engine and a Harness session.
It lets an agent session start, observe, resume and cancel typed Flow workflow
runs through a small set of tools, with durable run metadata kept in
caller-supplied storage. Durable storage and concrete transport adapters are
supplied by callers; this module does not select a backend.

## Status

Released. The root package provides the definition catalog
(`Catalog`, `TypedDefinition[S]`, strict JSON-Schema input/resume validation),
the durable run registry (`RunRegistry`, over `storage.KV`), the input store
(`InputStore`, over `storage.Blobs`), and the session-owned `Supervisor`
(checkpoint coordination, restart adoption and activity reconciliation, a
`storage.Leaser` lease, and bounded activity history). The `tools` package
exposes the bridge to a Harness agent as tools.

Known limits:

- There is no production workflow in this module. `internal/testworkflow` and
  the bridge integration tests are test fixtures that exercise the composition
  with memory providers.
- `SupervisorResourceName` is `"policy53-workflow-supervisor"`, a name inherited
  from Policy53, the first consumer.

## Install

```sh
go get github.com/looprig/workflows@latest
```

## Packages

| Package | Purpose |
|---|---|
| `github.com/looprig/workflows` | Catalog, typed definitions, run registry, input store, supervisor and their typed errors. |
| `github.com/looprig/workflows/tools` | `tools.NewBundle` returns the Harness tools `workflow_definition_list`, `workflow_run_start`, `workflow_run_get`, `workflow_run_list`, `workflow_run_history`, `workflow_run_resume` and `workflow_run_cancel`, scoped to one session. |
| `examples/docs/stage18_workflows` | Runnable documentation example: a typed definition registered in a catalog. |
| `internal/testworkflow` | Test-only workflow fixture. |

## Usage

The bridge is storage-neutral. Callers provide the checkpoint adapter and
session-owned services at composition time:

```go
checkpoints := flowstore.New(ledger) // github.com/looprig/flow/store: storage.Ledger -> flow.CheckpointStore
registry, _ := workflows.NewRunRegistry(kv)
inputs, _ := workflows.NewInputStore(blobs)
supervisor, _ := workflows.NewSupervisor(workflows.SupervisorConfig{
    SessionID: sessionID, Catalog: catalog, Registry: registry,
    Inputs: inputs, Leaser: leaser,
})
```

`checkpoints` is what each `workflows.NewTypedDefinition` is built over, and
`catalog` is a `workflows.NewCatalog()` with those definitions registered.

The supervisor owns only bounded run metadata, checkpoint coordination, and
metadata-only workflow activities. Application artifacts, policy text, model
prompts/output, and report bytes remain in caller-owned stores. Register the
supervisor as the Harness session resource (it implements
`tool.SessionResource`) so session shutdown cancels its goroutines and releases
its lease.

## Where it sits

Tier 5 of the Looprig workspace. Production dependencies: `core`, `flow`,
`harness` and `storage`. `inference` and `sessionstore` are test-only (the
`harness_integration` fault injector decodes SessionStore's envelope).

## Development

The Go baseline is 1.26.8. Standalone tests:

```sh
GOWORK=off go test ./...
GOWORK=off go test -tags harness_integration -run '^TestHarness' ./...   # Harness composition proofs
```

Make targets run with `GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local
GOFLAGS=-mod=readonly`, so the pinned module graph must already be in the
local module cache (`GOWORK=off go mod download`). Useful targets: `test`,
`race`, `integration`, `recovery`, `harness-integration`,
`harness-integration-race`, `standalone-check`, `check`, and
`release-checkpoint`, described below.

## Local release provenance

Any direct local replacement in `go.mod` would be part of the exact source set
used by a standalone Workflows checkpoint. The published `go.mod` has none, but
the repository provenance helper discovers replacements without resolving the
network and records one evidence record for Workflows and for each replacement
it finds.

Each record binds the canonical module path and `go.mod` SHA-256 to its Git
root, `HEAD` commit, repository tree, module-subdirectory tree, clean state,
and a bounded `git status --porcelain` evidence file. `make provenance` takes
pre- and post-gate captures, verifies their hashes and evidence files, and
fails closed if any repository is unavailable, dirty, or drifted. The default
status bound is 64 KiB; `WORKFLOWS_REPOSITORY_STATUS_MAX_BYTES` may lower it
but cannot raise the hard 1 MiB ceiling.

The retained `WORKFLOWS_PROVENANCE_DIR` contains `repositories/pre/`,
`repositories/post/`, `repositories.json`, and their status/evidence paths.
`provenance.json` embeds the verified repository record and its SHA-256.
Run `make repository-provenance-test` for deterministic mocked clean,
dirty, unavailable, and source/replacement drift coverage.

## Offline quality gates

`make check` is the non-mutating quality gate for this module. It runs
`fmt-check`, `vet`, the ordinary and race test suites, the bridge integration
tests, pinned `staticcheck`, `gosec`, `govulncheck`, a `-trimpath` build, and
module provenance checks. Every gate is explicit; an unavailable tool,
module, integration dependency, or vulnerability database is an error rather
than a skipped check.

The normal targets set `GOWORK=off`, `GOPROXY=off`, `GOSUMDB=off`,
`GOTOOLCHAIN=local`, and `-mod=readonly`. Before running `make check`, provide
an already-staged local Go vulnerability database:

```sh
POLICY53_GOVULNDB=/absolute/path/to/vulndb-v1 make check
```

The database must contain the local `index/modules.json` (or its gzipped
form). Network database URLs are rejected. The tagged
`harness-integration` and `harness-integration-race` composition proofs are
required prerequisites of `provenance` and `release-checkpoint`; like every
other target they run with `GOWORK=off` and proxy and checksum resolution
disabled, resolving the test-only `inference` dependency from `go.mod`. Their
successful results are recorded in `provenance.json` as the required full
Harness integration evidence: the selector covers every tagged
`TestHarness...` restore, publisher, cursor-CAS, lease-loss, and fault test.

`dependency-check` runs `go mod verify` and resolves the complete module graph
offline. `dependency-policy` additionally requires a reviewed offline
license-scanner lock, an exact receipt for the configured regular
non-symlinked executable, forbidden-dependency scanning, and a complete
license report. The lock, rather than the receipt, pins the immutable source
revision/source digest, distribution kind/digest, executable digest, exact
invocation and schemas, and canonical receipt identity. The current shared
lock is explicitly unsupported until real scanner inputs are reviewed, so a
local executable cannot make the gate pass. For a reviewed archive lock,
`WORKFLOWS_LICENSE_SCANNER_DISTRIBUTION` identifies the exact local artifact;
standalone executable locks bind the distribution digest to the configured
bytes. Signature metadata is recorded as unsupported because no repository
signature convention or verifier is available.

The receipt must match those lock-pinned values, the canonical executable
path, lock SHA-256, offline network declaration, and exact invocation. The
report must repeat the same scanner identity, source and distribution/executable
digests, canonical receipt identity, lock digest, invocation, and the SHA-256
identity of the exact receipt file before its module/license set is accepted;
a merely nonempty approved report is insufficient. `notices-check` verifies the committed notice
boundary. `provenance` retains two compared offline module inventories, their
SHA-256 digests, the `go mod verify` result, and a structured `provenance.json`
record containing the deterministic Go commands, toolchain, all tagged
Harness integration results (ordinary and race), and any explicitly supplied
build command/flags. Set
`WORKFLOWS_PROVENANCE_DIR` to an absolute private output directory to retain
the evidence, or set `WORKFLOWS_PROVENANCE_EXPECTED` to compare it with a
previously reviewed inventory. This module does not own Policy53’s NIST,
AnyDoc, or render locks. In the workspace, point
`WORKFLOWS_LICENSE_SCANNER_LOCK` at the reviewed scanner lock (or provide an
equivalent reviewed lock explicitly); missing scanner inputs fail closed.

For the release/checkpoint path, run `make release-checkpoint`. Its wrapper
captures repository evidence before the ordinary format, test, race,
integration, tool, and build gates; `provenance` then runs the dependency,
notice, and both complete tagged Harness integration suites before taking the
post-capture.
The checkpoint cannot report success if any source or replacement repository
is unavailable, dirty, or drifted. Both tagged Harness commands are recorded
as `PASS` only after their normal and race tests have completed successfully.
The ordinary `harness-integration` target remains available for the bounded
non-race run; both tagged targets select `^TestHarness` and retain their
90-second and 180-second timeouts.

## License

Apache License 2.0. See [LICENSE](LICENSE). Third-party notices are in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
