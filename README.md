# Workflows

`github.com/looprig/workflows` is an independent Go module for the
storage-neutral Flow-to-Harness workflow bridge. Durable storage and concrete
transport adapters are supplied by callers; this module does not select a
backend.

The module is intentionally scaffolded here. Workflow definitions and bridge
implementations are added by later tasks in the Policy53 implementation plan.

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
