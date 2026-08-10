# Workflows

`github.com/looprig/workflows` is an independent Go module for the
storage-neutral Flow-to-Harness workflow bridge. Durable storage and concrete
transport adapters are supplied by callers; this module does not select a
backend.

The bridge is storage-neutral. Callers provide the checkpoint adapter and
session-owned services at composition time:

```go
checkpoints := flowstore.New(ledger) // storage.Ledger -> flow.CheckpointStore
registry, _ := workflows.NewRunRegistry(kv)
inputs, _ := workflows.NewInputStore(blobs)
supervisor, _ := workflows.NewSupervisor(workflows.SupervisorConfig{
    SessionID: sessionID, Catalog: catalog, Registry: registry,
    Inputs: inputs, Leaser: leaser,
})
```

The supervisor owns only bounded run metadata, checkpoint coordination, and
metadata-only workflow activities. Application artifacts, policy text, model
prompts/output, and report bytes remain in caller-owned stores. Register the
supervisor as the Harness session resource so session shutdown cancels its
goroutines and releases its lease.

The `internal/testworkflow` package and bridge integration tests exercise the
composition with memory providers, including restart/adoption and activity
reconciliation recovery. They are test fixtures, not a production workflow.

## Local release provenance

The following sibling revisions are the development inputs for this scaffold.
The replace directives are local-only and are not a release dependency policy.

| module | path | revision |
| --- | --- | --- |
| Flow | `../flow` | `53636cff08e184ae325a8e5225d9a4aa02563581` |
| Harness | `../harness` | `3bc68a90e007b4ee7fc19c57e87fb1c00969ae9b` |
| Inference | `../inference` | `5d059abbf0275a6ae2b0bd3105b98b8ec09b8e8c` |
| Sandbox | `../sandbox` | `c96750fe44ff12d3257b385fe514023a000f86bc` |
| Storage | `../storage` | `e7cdd7ea32fd4b8dba87822cc97e3a55d65668fd` |
