#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
readonly TMP_DIR="$(cd -- "$(mktemp -d "${TMPDIR:-/tmp}/workflows-harness-gates-test.XXXXXX")" && pwd -P)"
trap 'rm -rf -- "$TMP_DIR"' EXIT
readonly MOCK_GO="$TMP_DIR/go"
readonly LOG="$TMP_DIR/go.log"

cp -- "$SCRIPT_DIR/testdata/mock-harness-go.sh" "$MOCK_GO"
chmod 700 "$MOCK_GO"

run_target() {
	local target=$1
	: >"$LOG"
	WORKFLOWS_HARNESS_GATE_LOG="$LOG" \
		make --no-print-directory -C "$ROOT" GO="$MOCK_GO" "$target"
}

expect_log() {
	local target=$1
	local expected=$2
	run_target "$target" >/dev/null
	if ! grep -F -x -- "$expected" "$LOG" >/dev/null; then
		printf 'harness gate tests: %s invoked unexpected Go arguments:\n' "$target" >&2
		cat "$LOG" >&2
		exit 1
	fi
	if grep -F -- 'TestHarnessSessionWorkflowActivityReplayMatchesAfterRestore' "$LOG" >/dev/null; then
		printf 'harness gate tests: %s retained the restore-only selector\n' "$target" >&2
		exit 1
	fi
}

expect_log harness-integration 'test ./... -tags harness_integration -run ^TestHarness -count=1 -timeout=90s'
expect_log harness-integration-race 'test -race ./... -tags harness_integration -run ^TestHarness -count=1 -timeout=180s'

if ! grep -F -- "--arg harness_test_pattern '^TestHarness'" "$ROOT/Makefile" >/dev/null; then
	printf 'harness gate tests: retained provenance pattern is missing\n' >&2
	exit 1
fi
if ! grep -F -- 'harness_integration: {required: true' "$ROOT/Makefile" >/dev/null; then
	printf 'harness gate tests: retained provenance scope is missing\n' >&2
	exit 1
fi
if grep -F -- 'harness_restore:' "$ROOT/Makefile" >/dev/null; then
	printf 'harness gate tests: provenance still claims restore-only evidence\n' >&2
	exit 1
fi

printf 'harness gate tests: PASS\n'
