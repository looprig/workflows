#!/usr/bin/env bash
set -Eeuo pipefail

: "${WORKFLOWS_HARNESS_GATE_LOG:?}"
printf '%s\n' "$*" >>"$WORKFLOWS_HARNESS_GATE_LOG"
